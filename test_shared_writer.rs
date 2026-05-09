use rustyline_async::SharedWriter;
fn print_event(stdout: &SharedWriter) {
    use std::io::Write;
    let mut out = stdout.clone();
    let _ = writeln!(out, "test");
}
