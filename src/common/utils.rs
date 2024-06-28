use crate::common::*;

pub fn data_file(id: &str) -> String {
    format!("data-{}.txt", id)
}

#[allow(dead_code)]
pub fn get_endpoint(addr: &str) -> Result<Endpoint, Box<dyn std::error::Error>> {
    let uri = Uri::builder().scheme("http").authority(addr).path_and_query("/").build()?;
    Ok(Endpoint::from_shared(uri.to_string())?)
}
