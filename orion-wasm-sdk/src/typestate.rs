/// Marker type representing the headers phase of an HTTP request or response.
pub struct HttpHeaders;
/// Marker type representing the body phase of an HTTP request or response.
pub struct HttpBody;

pub trait State {}
impl State for HttpHeaders {}
impl State for HttpBody {}
