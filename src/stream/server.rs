use std::fs::File;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use tiny_http::{Response, Server};

pub struct MediaServer {
    current_file: Arc<Mutex<Option<PathBuf>>>,
    port: u16,
    host_ip: Option<std::net::IpAddr>,
}

impl MediaServer {
    pub fn new(port: u16, host_ip: Option<std::net::IpAddr>) -> Self {
        let current_file = Arc::new(Mutex::new(None));
        let server_file = Arc::clone(&current_file);

        thread::spawn(move || {
            let server = Server::http(format!("0.0.0.0:{}", port)).unwrap();
            for request in server.incoming_requests() {
                let file_path = server_file.lock().unwrap();
                if let Some(path) = &*file_path {
                    if let Ok(file) = File::open(path) {
                        let response = Response::from_file(file);
                        let _ = request.respond(response);
                    }
                }
            }
        });

        Self {
            current_file,
            port,
            host_ip,
        }
    }

    pub fn host_file(&self, path: PathBuf) {
        let mut guard = self.current_file.lock().unwrap();
        *guard = Some(path);
    }

    pub fn get_url(&self) -> String {
        let ip = self.host_ip.unwrap_or_else(|| local_ip_address::local_ip().unwrap());
        format!("http://{}:{}/media", ip, self.port)
    }
}
