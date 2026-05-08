use std::io;
use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;
use std::time::Duration;

use mdns_sd::{ResolvedService, ServiceDaemon, ServiceEvent, ServiceInfo};
use uuid::Uuid;

use crate::error::Result;

const SERVICE_TYPE: &str = "_synkrophase._udp.local.";
const BROWSE_WINDOW: Duration = Duration::from_millis(250);

pub struct Discovery {
    daemon: ServiceDaemon,
    registered_fullname: Mutex<Option<String>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionInfo {
    pub room_code: String,
    pub leader_addr: SocketAddr,
    pub leader_id: Uuid,
}

impl Discovery {
    pub fn new() -> Result<Self> {
        let daemon = ServiceDaemon::new().map_err(to_network_error)?;
        Ok(Self {
            daemon,
            registered_fullname: Mutex::new(None),
        })
    }

    pub fn register_session(&self, room_code: &str, leader_id: Uuid, port: u16) -> Result<()> {
        let instance_name = format!("{room_code}.{leader_id}");

        let service_info = ServiceInfo::new(
            SERVICE_TYPE,
            &instance_name,
            &format!("{instance_name}.local."),
            "",
            port,
            None,
        )
        .map_err(to_network_error)?;

        let fullname = service_info.get_fullname().to_string();
        self.daemon
            .register(service_info)
            .map_err(to_network_error)?;
        *self.registered_fullname.lock().unwrap() = Some(fullname);

        Ok(())
    }

    pub fn find_sessions(&self) -> Result<Vec<SessionInfo>> {
        let receiver = self.daemon.browse(SERVICE_TYPE).map_err(to_network_error)?;
        let mut sessions = Vec::new();

        loop {
            match receiver.recv_timeout(BROWSE_WINDOW) {
                Ok(ServiceEvent::ServiceResolved(info)) => {
                    if let Some(session) = Self::resolved_to_session(&info) {
                        sessions.push(session);
                    }
                }
                Ok(_) => {}
                Err(_) => break,
            }
        }

        Ok(sessions)
    }

    pub fn unregister(&self) -> Result<()> {
        let Some(fullname) = self.registered_fullname.lock().unwrap().take() else {
            return Ok(());
        };

        let receiver = self
            .daemon
            .unregister(&fullname)
            .map_err(to_network_error)?;
        receiver
            .recv_timeout(BROWSE_WINDOW)
            .map_err(|err| to_network_error(err.to_string()))?;
        Ok(())
    }

    fn resolved_to_session(info: &ResolvedService) -> Option<SessionInfo> {
        let (room_code, leader_id) = Self::parse_fullname(info.get_fullname())?;
        let leader_ip = first_ip(info)?;
        Some(SessionInfo {
            room_code,
            leader_addr: SocketAddr::new(leader_ip, info.get_port()),
            leader_id,
        })
    }

    fn parse_fullname(fullname: &str) -> Option<(String, Uuid)> {
        let instance = fullname.strip_suffix(&format!(".{SERVICE_TYPE}"))?;
        let (room_code, leader_id) = instance.rsplit_once('.')?;
        let leader_id = Uuid::parse_str(leader_id).ok()?;
        Some((room_code.to_string(), leader_id))
    }
}

fn first_ip(info: &ResolvedService) -> Option<IpAddr> {
    info.get_addresses_v4().into_iter().next().map(IpAddr::V4)
}

fn to_network_error<E>(error: E) -> crate::error::SynkroError
where
    E: ToString,
{
    crate::error::SynkroError::Network(io::Error::other(error.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_fullname_extracts_room_and_leader() {
        let leader_id = Uuid::parse_str("00000000-0000-0000-0000-000000000123").unwrap();
        let parsed =
            Discovery::parse_fullname(&format!("ROOM42.{leader_id}.{SERVICE_TYPE}")).unwrap();

        assert_eq!(parsed, ("ROOM42".into(), leader_id));
    }

    #[test]
    fn parse_fullname_rejects_invalid_service_name() {
        assert!(Discovery::parse_fullname("ROOM42.invalid-service").is_none());
    }
}
