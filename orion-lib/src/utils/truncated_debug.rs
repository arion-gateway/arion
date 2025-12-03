#[derive(Clone, Copy)]
pub struct TruncatedDebug<'a, T, const N: usize>(pub &'a T);

impl<'a, T: std::fmt::Debug, const N: usize> std::fmt::Debug for TruncatedDebug<'a, T, N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let data_string = format!("{:?}", self.0);
        let max_len = N;

        if data_string.chars().count() > max_len {
            let truncated: String = data_string.chars().take(max_len).collect();
            f.write_fmt(format_args!("{}… ", truncated))
        } else {
            f.write_str(&data_string)
        }
    }
}
