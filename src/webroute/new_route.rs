/*
@author: xiao cai niao
@datetime: 2019/12/27
*/
use serde::Serialize;
use serde::Deserialize;
use actix_web::{web, HttpResponse};
use crate::storage::rocks::{DbInfo, PrefixTypeCode};
use crate::storage::opdb::{ClusterNodeInfo, NodeClusterList, RouteClusterList, SlaveBehindSetting};
use crate::webroute::response::{response_value, ResponseState};
use crate::webroute::op_value::ClusterMonitorInfo;
use crate::ha::sys_manager::MonitorSetting;
use crate::ha::procotol::MysqlMonitorStatus;

pub fn get_cluster_list(data: web::Data<DbInfo>) -> HttpResponse {
    let mut respons_list = NodeClusterList::new();
    if let Err(e) = respons_list.init(&data){
        return ResponseState::error(e.to_string())
    };
    response_value(&respons_list)
}

pub fn get_route_cluster_list(data: web::Data<DbInfo>) -> HttpResponse {
    let mut respons_list = RouteClusterList::new();
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

///
/// 配置slave 延迟检查
pub fn slave_delay_setting(data: web::Data<DbInfo>, info: web::Json<SlaveBehindSetting>) -> HttpResponse{
    if let Err(e) = info.save(&data){
        return ResponseState::error(e.to_string());
    }
    ResponseState::ok()
}

///
/// 获取slave延迟配置
pub fn get_slave_delay_setting(data: web::Data<DbInfo>, info: web::Json<PostCluster>) -> HttpResponse{
    let result = data.get_hehind_setting(&info.cluster_name);
    match result {
        Ok(v) =>{
            return response_value(&v);
        }
        Err(e) => {
            return ResponseState::error(e.to_string());
        }
    }
}


///
/// 获取节点监控配置
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct PostMonitorHost{
    pub host: String
}
pub fn get_monitor_setting(data: web::Data<DbInfo>, info: web::Json<PostMonitorHost>) -> HttpResponse{
    let result = data.prefix_get(&PrefixTypeCode::NodeMonitorSeting, &info.host);
    match result {
        Ok(v) => {
            if v.value.len()> 0{
                let value: MonitorSetting = serde_json::from_str(&v.value).unwrap();
                return response_value(&value);
            }
            let e = format!("host({}) no configuration information exists", &info.host);
            return ResponseState::error(e);
        }
        Err(e) =>{
            return ResponseState::error(e.to_string());
        }
    }
}


///
/// 修改节点监控配置
pub fn set_monitor_setting(data: web::Data<DbInfo>, info: web::Json<MonitorSetting>) -> HttpResponse{
    let result = info.save(&data);
    match result {
        Ok(_)  => {
            return ResponseState::ok();
        }
        Err(e) => {
            return ResponseState::error(e.to_string());
        }
    }
}


///
/// 用于统计集群内所有节点监控数据之和
#[derive(Serialize, Deserialize, Clone)]
pub struct ResponseMonitorStatic{
    pub current_qps: usize,
    pub slow_queries: usize,
    pub thread_running: usize,
    pub thread_connected: usize,
    pub com_insert: usize,
    pub com_update: usize,
    pub com_delete: usize,
    pub com_select: usize,
}
impl ResponseMonitorStatic{
    fn new() -> ResponseMonitorStatic{
        ResponseMonitorStatic{
            current_qps: 0,
            slow_queries: 0,
            thread_running: 0,
            thread_connected: 0,
            com_insert: 0,
            com_update: 0,
            com_delete: 0,
            com_select: 0
        }
    }

    pub fn update(&mut self, ms: &MysqlMonitorStatus) {
        self.current_qps += ms.questions;
        self.slow_queries += ms.slow_queries;
        self.thread_running += ms.threads_running;
        self.thread_connected += ms.threads_connected;
        self.com_select += ms.com_select;
        self.com_delete += ms.com_delete;
        self.com_update += ms.com_update;
        self.com_insert += ms.com_insert;
    }

    pub fn get_total_a(&self) -> ResponseMonitorA{
        ResponseMonitorA{
            current_qps: self.current_qps.clone(),
            slow_queries: self.slow_queries.clone(),
            thread_running: self.thread_running.clone(),
            thread_connected: self.thread_connected.clone(),
        }
    }

    pub fn get_total_b(&self) -> ResponseMonitorB{
        ResponseMonitorB{
            com_insert: self.com_insert.clone(),
            com_update: self.com_update.clone(),
            com_delete: self.com_delete.clone(),
            com_select: self.com_select.clone()
        }
    }
}
///
/// 集群信息页展示
#[derive(Serialize, Deserialize)]
pub struct ResponseMonitorA{
    pub current_qps: usize,
    pub slow_queries: usize,
    pub thread_running: usize,
    pub thread_connected: usize,
}
///
/// 路由信息页展示
#[derive(Serialize, Deserialize)]
pub struct ResponseMonitorB{
    pub com_insert: usize,
    pub com_update: usize,
    pub com_delete: usize,
    pub com_select: usize,
}

///
/// 集群信息页展示监控信息
pub fn get_cluster_total_monitor(data: web::Data<DbInfo>, info: web::Json<PostCluster>) -> HttpResponse{
    let mut cluster_info = ClusterNodeInfo::new(&info.cluster_name);
    if let Err(e) = cluster_info.init(&data){
        return ResponseState::error(e.to_string());
    }
    let mut rms = ResponseMonitorStatic::new();
    let res = cluster_info.static_monitor(&data, &mut rms);
    match res {
        Ok(_v) => {
            return response_value(&rms.get_total_a());
        }
        Err(e) => {
            return ResponseState::error(e.to_string());
        }
    }
}

///
/// 路由页展示对应的监控信息
pub fn get_cluster_total_monitor_route(data: web::Data<DbInfo>, info: web::Json<PostCluster>) -> HttpResponse{
    let mut cluster_info = ClusterNodeInfo::new(&info.cluster_name);
    if let Err(e) = cluster_info.init(&data){
        return ResponseState::error(e.to_string());
    }
    let mut rms = ResponseMonitorStatic::new();
    let res = cluster_info.static_monitor(&data, &mut rms);
    match res {
        Ok(_v) => {
            return response_value(&rms.get_total_b());
        }
        Err(e) => {
            return ResponseState::error(e.to_string());
        }
    }
}
