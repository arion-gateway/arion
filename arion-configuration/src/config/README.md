## Guiding principle

We aim for NG to be compatible with a subset of Envoy in order to ease adoption. Ideally, NG could serve as a drop-in replacement for Envoy and should be able to use the same control planes.

However, since we plan to only ever support a _subset_ of Envoy features there will be times where NG cannot serve as a drop-in replacement. In order to make sure that our users can trust NG we have to make them the following guarantee

> If NG accepts a configuration which was meant for Envoy, it will behave essentially like Envoy. If at any point we diverge from Envoys behaviour it will be documented and the reasoning will be explained. If NG cannot honour a config in the same way Envoy does, we will refuse to parse it (if loading from a file) or reject the update (if using xDS).

here _"essentially like Envoy"_ means that we will follow the behaviour of Envoy at a high-level. Filters will be executed in the expected order, request will be routed or rejected as expected, but there might be minor differences in less-crucial behaviours. Like exposed statistics, or implementation details of load balancing algorithms. Basically, it should be _safe_ to replace Envoy with NG.

This crate is responsible for upholding this guarantee at the config parser level. I makes invalid envoy configs unrepresentable in the code. `arion-lib` is responsible for implementing the correct behaviour on top of it.


## Dev-guide

### Destructuring
When implementing a function to convert from an envoy structure to a NG structure, always make sure to destructure the Envoy structure early on, like so:
```rs
fn try_from(envoy_struct: Envoy) -> T {
    let Envoy { name, enabled, filters, mode } = envoy_struct;
    // ...
}
```

That way, you know for sure that you are using all the fields of the struct, or you get an unused variable warning.

Fields which are not supported, and who should give an error if used, should be wrapped inside of a `unsupported_field!` macro.
Like so:
```rs
fn try_from(envoy_struct: Envoy) -> T {
    let Envoy { name, enabled, filters, mode } = envoy_struct;
    unsupported_field!(enabled, filters)?;
}
```
This macro takes a list of identifiers and will return a `Err(GenericError::UnsupportedField("$[field name]"))` if any are used.

Likewise, fields which are _required_ should be wrapped with `required!(field)` first. This macro will throw an error if a field was not filled out in the config. This applies to options of course but also applies to other structures such as `Vec` or `String` as the YAML/Protobuf parser will populate these fields with their default empty values.

For example:
```rs
fn try_from(envoy_struct: Envoy) -> T {
    let Envoy { name, enabled, filters, mode } = envoy_struct;
    unsupported_field!(enabled, filters)?;
    // if name == "" this will error out.
    let name = required!(name)?;
}
```

You should also apply this trick when parsing nested items, like structs inside of an enum:

```rs
fn try_from(envoy_struct: Envoy) -> T {
    let Envoy { name, enabled, filters, mode } = envoy_struct;
    unsupported_field!(enabled, filters)?;
    match mode {
        EnvoyMode::ModeA(EnvoyModeA{is_flarb,  total_blobs}) => {
            unsupported_field(total_blobs)?;
            required!(is_flarb)?;
            //...
        }
        EnvoyMode::SimpleBool(x: bool) => { /*...*/ }
        EnvoyMode::WrappedBool(EnvoyBool{value}) => { /*...*/ }
    }
}
```

### Determining which fields are required or unsupported

By default, you should mark _all_ fields as unsupported, and only selectively allow those fields which are handled by NG.
The guiding principle should be that _any config_ that is accepted by NG should produce the same behaviour it does in Envoy.

Some fields might have additional requirements that are not captured by the Envoy structure. Such as a vector or option being non-empty, or a string matching a specific regex. You can find these requirements as annotations in the Protobuf files on [Envoy's Github](https://github.com/envoyproxy/envoy/tree/main/api/envoy).

For example,
```protobuf
 // A port number that describes the destination port connecting to.
    uint32 destination_port = 6 [(validate.rules).uint32 = {lte: 65535}];
```
says that the field `destination_port` is a `u32` which has to be less-than-or-equal-to 65535.


 Sometimes we might need to add more complicated checks to ensure we behave as Envoy would do, such as making sure that a filter-chain has at most 1 rate-limiter, and it appears after any RBAC filters. It is crucial to include these checks, as accepting a config but behaving differently negatively affects user experience at best and could have serious security implications at worst.

### Other utility macros

`envoy.rs` contains a few utility macros to make all of this easier. We already saw `required!` and `unsupported_field!` but it also contains functions for converting a vec of structs and optionally checking that it is non-empty (`convert_vec!`/`convert_non_empty_vec!`) or converting an option and making sure it is `Some` (`convert_opt!`).

It is important to note that all these macros use the name of the first argument to generate their error string, so make sure the variable name matches the field in question.

```rs
let doesnt_matter = convert_opt(filter)?;//will say the field "filter" is missing on an error
let doesnt_matter = convert_opt(some_option.map(some_func))?; // don't
```

### Error handling

This crate heavily uses the error type `GenericError`. It is a small wrapper error that handles all errors that can occur during parsing (unsupported field used, unsupported enum variant used, required field is missing, a different error occurred while parsing a field - e.g. an invalid port number was set).

It builds a chain of fields that are currently being parsed through the use of the `Result<T,GenericError>::with_node(impl Into<Cow<'static, str>>)` function and `GenericError::with_node(impl Into<Cow<'static, str>>)` function.
When you enter a subfield of a struct, you should always add a `with_node()` to the end of the result, like so
(**note:** macros will add these automatically).

```rs
let filter = Filter::try_from(filter).with_node("filter")?;
let other_field = convert_opt(other_field)?; //macro automatically adds `with_node("other_field")`
```
if the struct you are parsing has a `name` field, you should also add it as a node as early as possible, like so:
```rs
fn try_from(envoy_struct: Envoy) -> T {
    let Envoy { name, enabled, filters, mode } = envoy_struct;
    let name = required!(name)?;
    {
        unsupported_field!(enabled, filters)?;
        // ...
        Ok(Self{ .. })
    }.with_node(name)
}
```

Sometimes, when doing a larger more complicated conversion you might find it hard to keep track of things or satisfy the borrow checker without an early return or heavy use of clones. One trick you can then employ is to define-and-call a closure like so:
```rs
(||->Result<_, GenericError>{
    //we should destructure here actually
    for (idx,item) in items.into_iter().enumerate() {
        if let Err(e) = complicated_check(item) {
            //this now returns from the closure, not the parent function
            return Err(GenericError::from_msg_with_cause(item.name.unwrap_or_else(||format!("[item {idx}]")),e));
        }
        //more logic
    }
})().with_node("items")?;
```

When parsing a field value you might encounter a different kind of error. For example, the config might contain a port number that is outside of the valid range, or try to set a header name from an invalid string.

In those cases you should use `GenericError::from_msg_with_cause` or `GenericError::from_msg` and the `msg` should be a string saying which _value_ failed to parse as _what_ and the `cause` should be the underlying error message, if any.

For example:
```rs
let header_name = http::HeaderName::from_str(header_name)
                    .map_err(|e| GenericError::from_str_with_cause(
                        format!("Failed to parse \"{header_name}\" as HeaderName"),e)
                    ).with_node("header_name")?;
// for this one, we don't include the error because the TryFromIntError does not tells us anything new
// whereas the header name one might say which character was invalid for example
let port = u16::try_from(port)
            .map_err(|_e| GenericError::from_msg(format!("\"{port}\" is not a valid port number")))
            .with_node(port)?;
```
