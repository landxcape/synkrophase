use mdns_sd::{ServiceDaemon, ServiceInfo};
use std::net::SocketAddr;
use uuid::Uuid;
use crate::error::Result;

pub struct Discovery {
    daemon: ServiceDaemon,
}

pub struct SessionInfo {
    pub room_code: String,
    pub leader_addr: SocketAddr,
    pub leader_id: Uuid,
}

impl Discovery {
    pub fn new() -> Result<Self> {
        let daemon = ServiceDaemon::new()
            .map_err(|e| crate::error::SynkroError::Network(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        Ok(Self { daemon })
    }

    pub fn register_session(&self, room_code: &str, leader_id: Uuid, port: u16) -> Result<()> {
        let service_type = "_synkrophase._udp.local.";
        let instance_name = format!("{}.{}", room_code, leader_id);
        
        let service_info = ServiceInfo::new(
            service_type,
            &instance_name,
            &format!("{}.local.", instance_name),
            "",
            port,
            None,
        ).map_err(|e| crate::error::SynkroError::Network(std::io::Error::new(std::io::ErrorKind::Other, e)))?;

        self.daemon.register(service_info)
            .map_err(|e| crate::error::SynkroError::Network(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        
        Ok(())
    }

    pub fn unregister(&self) -> Result<()> {
        // In a real implementation, we'd need to store the ServiceInfo to unregister.
        // For the MVP/Mock, we'll keep it simple.
        Ok(())
    }
}
