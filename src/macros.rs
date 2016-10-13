macro_rules! fail {
    ($out:expr, $err:expr, $debug:expr) => {{
        if $debug {
            writeln!($out, "{}: {}", "optail error".red(), $err).unwrap();
        }

        return;
    }}
}

macro_rules! get_or_fail {
    ($exp:expr, $out:expr, $debug:expr) => { match $exp {
        Ok(val) => val,
        Err(e) => fail!($out, e, $debug),
    }}
}
