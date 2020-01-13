/*
@author: xiao cai niao
@datetime: 2020/01/06
*/
use actix_web::web;
use actix_web::HttpResponse;
use crate::storage::rocks::{DbInfo, CfNameTypeCode, RowValue, PrefixTypeCode};
use crate::webroute::response::{ResponseState, response_value};
use std::error::Error;
use crate::ha::sys_manager::MonitorSetting;
use crate::storage::opdb::HostInfoValue;
use std::str::from_utf8;
use crate::ha::procotol::MysqlMonitorStatus;
use serde::{Serialize, Deserialize};


#[derive(Serialize, Deserialize, Clone)]
pub struct ClusterMonitorMetric{
    cluster_name: String,
    node_list: Vec<String>
}
impl ClusterMonitorMetric{
    fn new(cluster_name: &String) -> ClusterMonitorMetric{
        ClusterMonitorMetric{ cluster_name: cluster_name.clone(), node_list: vec![] }
    }

    fn init(&mut self, ms: &Vec<RowValue<MonitorSetting>>, host: &String) {
        if self.is_exists(host){return;}
        if !self.check_monitor(ms, host){return;}
        self.node_list.push(host.clone());
    }

    fn check_monitor(&self, ms: &Vec<RowValue<MonitorSetting>>, host: &String) -> bool{
        for rw in ms{
            if &rw.value.host == host{
                if rw.value.monitor{
                    return true;
                }
            }
        }
        return false;
    }

    fn is_exists(&self, host: &String) -> bool{
        for h in &self.node_list{
            if h == host{
                return true;
            }
        }
        return false;
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ResponseClusterMetric{
    metric_info: Vec<String>,
    nodes_info: Vec<ClusterMonitorMetric>
}
impl ResponseClusterMetric{
    fn new() -> ResponseClusterMetric{
        let metric_info = vec!["com_insert".to_string(),
                               "com_update".to_string(),
                               "com_delete".to_string(),
                               "com_select".to_string(),
                               "questions".to_string(),
                               "innodb_row_lock_current_waits".to_string(),
                               "innodb_row_lock_time".to_string(),
                               "created_tmp_disk_tables".to_string(),
                               "created_tmp_tables".to_string(),
                               "innodb_buffer_pool_reads".to_string(),
                               "innodb_buffer_pool_read_requests".to_string(),
                               "handler_read_first".to_string(),
                               "handler_read_key".to_string(),
                               "handler_read_next".to_string(),
                               "handler_read_prev".to_string(),
                               "handler_read_rnd".to_string(),
                               "handler_read_rnd_next".to_string(),
                               "innodb_os_log_pending_fsyncs".to_string(),
                               "innodb_os_log_pending_writes".to_string(),
                               "innodb_log_waits".to_string(),
                               "threads_connected".to_string(),
                               "threads_running".to_string(),
                               "bytes_sent".to_string(),
                               "bytes_received".to_string(),
                               "slow_queries".to_string()];
        ResponseClusterMetric{ metric_info, nodes_info: vec![] }
    }
    fn init(&mut self, db: &web::Data<DbInfo>, ms: &Vec<RowValue<MonitorSetting>>) -> Result<(), Box<dyn Error>>{
        let result = db.iterator(&CfNameTypeCode::HaNodesInfo.get(),&"".to_string())?;
        'all: for row in &result{
            if row.value.len() == 0 {continue;}
            let node_info: HostInfoValue = serde_json::from_str(&row.value)?;
            'o: for cl_info in &mut self.nodes_info{
                if cl_info.cluster_name == node_info.cluster_name{
                    cl_info.init(ms, &node_info.host);
                    continue 'all;
                }
            }

            let mut cnm = ClusterMonitorMetric::new(&node_info.cluster_name);
            cnm.init(ms,&node_info.host);
            self.nodes_info.push(cnm);
        }
        Ok(())
    }
}

pub fn get_cluster_metric(data: web::Data<DbInfo>) -> HttpResponse {
    let monitor_setting = data.get_monitor_setting();
    match monitor_setting{
        Err(e) => {
            return ResponseState::error(e.to_string());
        }
        Ok(a) =>{
            let mut rsm = ResponseClusterMetric::new();
            if let Err(e) = rsm.init(&data, &a){
                return ResponseState::error(e.to_string());
            };
            return response_value(&rsm);
        }
    }
}

impl MysqlMonitorStatus{
    fn get_value(&self, metric: &String) -> usize{
        if metric == "com_insert"{
            return self.com_insert;
        }else if metric == "com_update" {
            return self.com_update;
        }else if metric == "com_delete" {
            return self.com_delete;
        }else if metric == "com_select" {
            return self.com_select;
        }else if metric == "questions" {
            return self.questions;
        }else if metric == "innodb_row_lock_current_waits" {
            return self.innodb_row_lock_current_waits;
        }else if metric == "innodb_row_lock_time" {
            return self.innodb_row_lock_time;
        }else if metric == "created_tmp_disk_tables" {
            return  self.created_tmp_disk_tables;
        }else if metric == "created_tmp_tables"{
            return self.created_tmp_tables;
        }else if metric == "innodb_buffer_pool_reads" {
            return self.innodb_buffer_pool_reads;
        }else if metric == "innodb_buffer_pool_read_requests" {
            return self.innodb_buffer_pool_read_requests;
        }else if metric == "handler_read_first" {
            return self.handler_read_first;
        }else if metric == "handler_read_key" {
            return self.handler_read_key;
        }else if metric == "handler_read_next" {
            return self.handler_read_next;
        }else if metric == "handler_read_prev" {
            return  self.handler_read_prev;
        }else if metric == "handler_read_rnd" {
            return self.handler_read_rnd;
        }else if metric == "handler_read_rnd_next" {
            return  self.handler_read_rnd_next;
        }else if metric == "innodb_os_log_pending_fsyncs"{
            return self.innodb_os_log_pending_fsyncs;
        }else if metric == "innodb_os_log_pending_writes"{
            return self.innodb_os_log_pending_writes;
        }else if metric == "innodb_log_waits" {
            return self.innodb_log_waits;
        }else if metric == "threads_connected" {
            return self.threads_connected;
        }else if metric == "threads_running" {
            return self.threads_running;
        }else if metric == "bytes_sent"{
            return self.bytes_sent;
        }else if metric == "bytes_received" {
            return self.bytes_received;
        }else if metric == "slow_queries"{
            return self.slow_queries;
        }else {
            return 0;
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct MetricValue{
    metric: String,
    value: Vec<Vec<usize>>   // [time, value]
}
impl MetricValue{
    fn new(metric: &String) -> MetricValue{
        MetricValue{ metric: metric.clone(), value: vec![] }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ResponseMonitorMetricValue{
    monitor_value: Vec<MetricValue>
}
impl ResponseMonitorMetricValue{
    fn new(metric_list: &Vec<String>) -> ResponseMonitorMetricValue{
        let mut m_list = vec![];
        for metric in metric_list{
            let metricvalue = MetricValue::new(&metric);
            m_list.push(metricvalue)
        }
        ResponseMonitorMetricValue{ monitor_value: m_list }
    }

    fn init(&mut self, value: &MysqlMonitorStatus) {
        for metricv in &mut self.monitor_value{
            let mv = vec![value.time.clone() as usize, value.get_value(&metricv.metric)];
            metricv.value.push(mv);
        }
    }
}

///
/// web端拉取监控数据的请求
#[derive(Serialize, Deserialize, Clone)]
pub struct PostMonitorMetricValue{
    host: String,
    metric: Vec<String>,
    start_time: i64,
    stop_time: i64
}
impl PostMonitorMetricValue{
    fn get_value(&self, db: &web::Data<DbInfo>) -> Result<ResponseMonitorMetricValue, Box<dyn Error>>{
        let cf_name = CfNameTypeCode::SystemData.get();
        let mut rmmv = ResponseMonitorMetricValue::new(&self.metric);
        let prefix = format!("{}:{}", PrefixTypeCode::NodeMonitorData.prefix(), &self.host);
        if let Some(cf) = db.db.cf_handle(&cf_name) {
            let iter = db.db.prefix_iterator_cf(cf,&prefix)?;
            for (k, v) in iter {
                let key: String = from_utf8(&k.to_vec())?.parse()?;
                if !key.starts_with(&prefix){continue;}
                let value: MysqlMonitorStatus = serde_json::from_slice(&v.to_vec())?;
                if !self.check_time(&value){continue;}
                rmmv.init(&value)
            }
            return Ok(rmmv);
        }
        let a = format!("no cloumnfamily {}", cf_name);
        return Err(a.into())
    }

    fn check_time(&self, value: &MysqlMonitorStatus) -> bool{
        if value.time < self.start_time{
            return false;
        }
        if value.time > self.stop_time{
            return false;
        }
        return true;
    }
}

pub fn get_metric_value(data: web::Data<DbInfo>, info: web::Json<PostMonitorMetricValue>) -> HttpResponse{
    let result = info.get_value(&data);
    match result {
        Ok(v) => {
            return response_value(&v)
        }
        Err(e) => {
            return ResponseState::error(e.to_string())
        }
    }
}