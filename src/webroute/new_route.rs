/*
@author: xiao cai niao
@datetime: 2019/12/27
*/
use serde::Serialize;
use serde::Deserialize;
use actix_web::{web, HttpResponse};
use crate::storage::rocks::{DbInfo};
use crate::storage::opdb::{ ClusterNodeInfo, NodeClusterList};
use crate::webroute::response::{response_value, ResponseState};
use crate::webroute::op_value::ClusterMonitorInfo;

pub fn get_cluster_list(data: web::Data<DbInfo>) -> HttpResponse {
    let mut respons_list = NodeClusterList::new();
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
    response_value(&monitor_info)
}