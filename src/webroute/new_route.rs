/*
@author: xiao cai niao
@datetime: 2019/12/27
*/
use serde::Serialize;
use serde::Deserialize;
use actix_web::{web, HttpResponse};
use crate::storage::rocks::{DbInfo, CfNameTypeCode};
use crate::storage::opdb::{HostInfoValue, ClusterNodeInfo};
use std::error::Error;
use crate::webroute::response::{response_value, ResponseState};
use crate::webroute::op_value::ClusterMonitorInfo;


///
/// web端拉取所有集群名列表
#[derive(Serialize, Deserialize, Debug)]
pub struct ResponseClusterList {
    cluster_name_list: Vec<String>
}

impl ResponseClusterList{
    fn new() -> ResponseClusterList{
        ResponseClusterList{ cluster_name_list: vec![] }
    }

    fn init(&mut self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>> {
        let result = db.iterator(&CfNameTypeCode::HaNodesInfo.get(), &String::from(""))?;
        for row in &result{
            let value: HostInfoValue = serde_json::from_str(&row.value)?;
            if self.is_exists(&value.cluster_name){continue;}
            self.cluster_name_list.push(value.cluster_name.clone());
        }
        Ok(())
    }
    fn is_exists(&self, cluster_name: &String) -> bool {
        for cl in &self.cluster_name_list {
            if cl == cluster_name{
                return true;
            }
        }
        return false;
    }
}

pub fn get_cluster_list(data: web::Data<DbInfo>) -> HttpResponse {
    let mut respons_list = ResponseClusterList::new();
    if let Err(e) = respons_list.init(&data){
        return ResponseState::error(e.to_string())
    };
    response_value(&respons_list)
}


///
/// web端根据cluster_name拉去对应集群节点信息
#[derive(Serialize, Deserialize, Debug)]
pub struct PostCluster{
    pub cluster_name: String
}

pub fn get_cluster_node_info(data: web::Data<DbInfo>, info: web::Json<PostCluster>) -> HttpResponse {
    info!("{:?}", &info);
    let mut cluster_info = ClusterNodeInfo::new(&info.cluster_name);
    if let Err(e) = cluster_info.init(&data){
        return ResponseState::error(e.to_string());
    }
    response_value(&cluster_info)
}




///
/// 获取集群统计监控信息
pub fn get_cluster_monitor_status(data: web::Data<DbInfo>, info: web::Json<PostCluster>) -> HttpResponse{
    let mut monitor_info = ClusterMonitorInfo::new(&info.cluster_name);
    if let Err(e) = monitor_info.init(&data, &info.cluster_name){
        return ResponseState::error(e.to_string());
    }
    ResponseState::ok()
}