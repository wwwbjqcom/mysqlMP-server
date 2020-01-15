/*
@author: xiao cai niao
@datetime: 2019/12/27
*/
use serde::Serialize;
use serde::Deserialize;
use actix_web::{web, HttpResponse};
use crate::storage::rocks::{DbInfo, PrefixTypeCode, CfNameTypeCode, KeyValue};
use crate::storage::opdb::{ClusterNodeInfo, NodeClusterList, RouteClusterList, SlaveBehindSetting, HostInfoValue};
use crate::webroute::response::{response_value, ResponseState};
use crate::webroute::op_value::ClusterMonitorInfo;
use crate::ha::sys_manager::MonitorSetting;
use crate::ha::procotol::{MysqlMonitorStatus, MysqlState};
use std::error::Error;
use crate::ha::nodes_manager::CheckState;

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


///
///
/// 获取报警信息
#[derive(Serialize, Deserialize, Debug)]
pub struct PostAlter{
    pub hook_id: String,
}
#[derive(Serialize, Deserialize, Debug)]
pub struct ResponseDownNodeInfo{
    pub cluster_name: String,
    pub host: String,
    pub role: String,
    pub sql_thread: usize,       // 0 代表false, 1代表true
    pub io_thread: usize,
    pub db_down: usize,
    pub client_down: usize,
}
impl ResponseDownNodeInfo{
    fn new(info: &HostInfoValue) -> ResponseDownNodeInfo{
        ResponseDownNodeInfo{
            cluster_name: info.cluster_name.clone(),
            host: info.host.clone(),
            role: "".to_string(),
            sql_thread: 0,
            io_thread: 0,
            db_down: 0,
            client_down: 0
        }
    }

    fn init(&mut self, db: &DbInfo, state: &MysqlState) -> Result<(), Box<dyn Error>>{
        self.role = state.role.clone();
        if !state.sql_thread{
            self.sql_thread = 1
        }
//        self.sql_thread = state.sql_thread.clone();
//        self.io_thread = state.io_thread.clone();
        if !state.io_thread{
            self.io_thread = 1
        }
//        if !self.online{
//            self.check_down_state(db)?;
//        }
        Ok(())
    }

    fn check_down_state(&mut self, db: &DbInfo) -> Result<(), Box<dyn Error>>{
        let result = db.get(&self.host, &CfNameTypeCode::CheckState.get())?;
        //info!("{:?}",&result);
        if result.value.len() > 0 {
            let value: CheckState = serde_json::from_str(&result.value)?;
            if value.db_down{
                self.db_down = 1
            }
            if value.client_down{
                self.client_down = 1
            }
//            self.db_down = value.db_down;
//            self.client_down = value.client_down;
        }
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResponseAlter{
    pub nodes_info: Vec<ResponseDownNodeInfo>,
}

impl ResponseAlter{
    fn new(rows: &Vec<KeyValue>, db: &DbInfo) -> Result<ResponseAlter, Box<dyn Error>>{
        let mut nodes_info = vec![];
        for kv in rows{
            let state: HostInfoValue = serde_json::from_str(&kv.value)?;
            let mut rdi = ResponseDownNodeInfo::new(&state);
            if !state.online{
                rdi.check_down_state(db)?;
            }
            nodes_info.push(rdi);
        }
        Ok(ResponseAlter{
            nodes_info
        })
    }

    fn init(&mut self, db: &DbInfo) -> Result<(), Box<dyn Error>>{
        let cf_name = CfNameTypeCode::NodesState.get();
        for node in &mut self.nodes_info{
            let result = db.get(&node.host, &cf_name)?;
            let value: MysqlState = serde_json::from_str(&result.value)?;
            node.init(db, &value)?;
        }
        Ok(())
    }

}

impl DbInfo{
    ///
    /// 获取所有节点状态信息，用于监控报警
    fn get_all_node_state(&self) -> Result<ResponseAlter, Box<dyn Error>>{
        let cf_name = CfNameTypeCode::HaNodesInfo.get();
        let result = self.iterator(&cf_name, &"".to_string())?;
        let mut response_alter = ResponseAlter::new(&result, &self)?;
        response_alter.init(&self)?;
        Ok(response_alter)
    }
}

pub fn alter_interface(data: web::Data<DbInfo>, info: web::Json<PostAlter>) -> HttpResponse {
    if let Err(e) = data.check_user_info(&info.hook_id){
        return ResponseState::error(e.to_string());
    }
    match data.get_all_node_state() {
        Ok(response_alter) => {
            return response_value(&response_alter);
        }
        Err(e) => {
            return ResponseState::error(e.to_string());
        }
    }
}