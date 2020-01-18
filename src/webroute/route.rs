/*
@author: xiao cai niao
@datetime: 2019/11/5
*/
use actix_web::{web, HttpResponse};
use actix_session::{ Session};
use serde::{Deserialize, Serialize};
use crate::storage;
use crate::storage::rocks::{DbInfo, KeyValue, CfNameTypeCode, PrefixTypeCode};
use crate::ha::procotol::{MysqlState, CommandSql, MyProtocol};
use std::error::Error;
use crate::ha::nodes_manager::{SwitchForNodes, DifferenceSql, SqlRelation};
use crate::storage::opdb::{HaChangeLog, UserInfo, HostInfoValue};
use crate::ha::route_manager::RouteInfo;
use crate::webroute::response::{response_state, response_value, ResponseState};
use crate::webroute::new_route::PostCluster;
use crate::ha::sys_manager::MonitorSetting;


#[derive(Serialize, Deserialize, Debug)]
pub struct HostInfo {
    pub host: String,   //127.0.0.1:3306
    pub rtype: String,  //db、route
    pub dbport: usize, //default 3306
    pub cluster_name: String   //集群名称
}

/// extract `import host info` using serde
pub fn import_mysql_info(data: web::Data<DbInfo>, info: web::Json<HostInfo>) -> HttpResponse {
    let state = storage::opdb::insert_mysql_host_info(data, &info);
    return response_state(state);
}


#[derive(Serialize, Deserialize)]
pub struct EditInfo {
    pub cluster_name: String,
    pub host: String,
    pub dbport: usize,
    pub role: String,
    pub online: bool,
}

pub fn edit_nodes(data: web::Data<DbInfo>, info: web::Json<EditInfo>) -> HttpResponse {
    let cf_name = String::from("Ha_nodes_info");
    let key = &info.host;
    let cur_value = data.get(key, &cf_name);
    match cur_value {
        Ok(v) => {
            let mut db_value: HostInfoValue = serde_json::from_str(&v.value).unwrap();
            db_value.edit(&info);
            let value = serde_json::to_string(&db_value).unwrap();
            let row = KeyValue::new(&key, &value);
            let a = data.put(&row, &cf_name);
            return response_state(a);
        }
        Err(e) => {
            return ResponseState::error(e.to_string());
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct EditMainTain{
    pub host: String,
    pub maintain: String
}

pub fn edit_maintain(data: web::Data<DbInfo>, info: web::Json<EditMainTain>) -> HttpResponse {
    let cf_name = String::from("Ha_nodes_info");
    let key = &info.host;
    //检查master状态
    if let Ok(status) = data.get(&key, &CfNameTypeCode::NodesState.get()){
        if status.value.len() > 0 {
            let cur_status: MysqlState = serde_json::from_str(&status.value).unwrap();
            if cur_status.role == "master".to_string(){
                if cur_status.online{
                    return ResponseState::error("the master node cannot be set to maintenance mode".to_string());
                }
            }
        }
    };
    //设置模式
    let cur_value = data.get(key, &cf_name);
    match cur_value {
        Ok(v) => {
            //info!("{:?}", &v);
            let mut db_value: HostInfoValue = serde_json::from_str(&v.value).unwrap();
            db_value.maintain(&info);
            //info!("{:?}", &db_value);
            let value = serde_json::to_string(&db_value).unwrap();
            let row = KeyValue::new(&key, &value);
            let a = data.put(&row, &cf_name);
            return response_state(a);
        }
        Err(e) => {
            return ResponseState::error(e.to_string());
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct DeleteNode {
    pub host: String,
}

impl DeleteNode {
    pub fn exec(&self, data: &web::Data<DbInfo>) -> HttpResponse {
        let cf_name = String::from("Ha_nodes_info");
        let cur_value = data.get(&self.host, &cf_name);
        match cur_value {
            Ok(v) => {
                let value: HostInfoValue = serde_json::from_str(&v.value).unwrap();
                if value.maintain {
                    if let Err(e) = data.delete(&self.host, &cf_name){
                        return ResponseState::error(e.to_string());
                    }
                    //删除监控配置
                    let monitor_set = MonitorSetting::new(&self.host);
                    if let Err(e) = monitor_set.delete(data){
                        return ResponseState::error(e.to_string());
                    }

                    //删除状态信息
                    if let Err(e) = data.delete(&self.host, &CfNameTypeCode::NodesState.get()){
                        return ResponseState::error(e.to_string());
                    }

                    return ResponseState::ok();

                }else {
                    let err = String::from("the maintenance mode node is only deleted");
                    return ResponseState::error(err);
                }
            }
            Err(e) => {
                return ResponseState::error(e.to_string());
            }
        }
    }
}

pub fn delete_node(data: web::Data<DbInfo>, info: web::Json<DeleteNode>) -> HttpResponse {
    return info.exec(&data);
}

///
/// 主动切换
///
#[derive(Serialize, Deserialize, Debug)]
pub struct SwitchInfo {
    pub host: String,
}
pub fn switch(data: web::Data<DbInfo>, info: web::Json<SwitchInfo>) -> HttpResponse {
    info!("manually switch {} to master", info.host);
    let mut switch_info = SwitchForNodes::new(&info.host);
    return response_state(switch_info.switch(&data));
}

///
///
#[derive(Deserialize, Serialize)]
pub struct RowLog {
    row_key: String,
    data: HaChangeLog
}
#[derive(Deserialize, Serialize)]
pub struct SwitchLog{
    log_data: Vec<RowLog>
}

impl SwitchLog {
    fn new() -> SwitchLog {
        SwitchLog{log_data: vec![] }
    }

    fn get_all(&mut self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>> {
        let result = db.iterator(&CfNameTypeCode::HaChangeLog.get(), &String::from(""));
//        let host = String::from("10.0.1.112");
//        let result = db.prefix_iterator(&host, &CfNameTypeCode::HaChangeLog.get());
        match result {
            Ok(v) => {
                //v.sort_by(|a, b| b.key.cmp(&a.key));
                for row in v{
                    let value: HaChangeLog = serde_json::from_str(&row.value)?;
                    let row = RowLog{ row_key: row.key, data: value };
                    self.log_data.push(row);
                }
                return Ok(());
            }
            Err(e) => {
                return Err(e);
            }
        }
    }
}

pub fn switchlog(data: web::Data<DbInfo>) -> HttpResponse {

    let mut switch_log = SwitchLog::new();
    if let Err(e) = switch_log.get_all(&data) {
        return ResponseState::error(e.to_string());
    }
    return response_value(&switch_log);
}


///
/// 返回路由信息
#[derive(Serialize, Deserialize)]
pub struct ResponseRouteInfo {
    pub route: Vec<RouteInfo>
}

///
/// 获取集群对应路由关系的请求包
#[derive(Serialize, Deserialize)]
pub struct GetRouteInfo {
    pub hook_id: String,
    pub clusters: Vec<String>,
}

impl GetRouteInfo {
    pub fn getall(&self, db: &web::Data<DbInfo>) -> Result<ResponseRouteInfo, Box<dyn Error>> {
        let mut res_route = ResponseRouteInfo{route: vec![]};
        let result = db.prefix_iterator(&PrefixTypeCode::RouteInfo.prefix(), &CfNameTypeCode::SystemData.get())?;
        for kv in result {
            if !kv.key.starts_with(&PrefixTypeCode::RouteInfo.prefix()){
                continue;
            }
            if kv.value.len() > 0{
                let value: RouteInfo = serde_json::from_str(&kv.value)?;
                res_route.route.push(value);
            }
        }
        Ok(res_route)
    }

    pub fn get(&self, db: &web::Data<DbInfo>) -> Result<ResponseRouteInfo, Box<dyn Error>> {
//        self.check_user_info(db)?;
        db.check_user_info(&self.hook_id)?;
        let mut res_route = ResponseRouteInfo{route: vec![]};
        for cluster in &self.clusters{
            let kv = db.prefix_get(&PrefixTypeCode::RouteInfo, cluster)?;
            if kv.value.len() == 0 {continue;}
            let value: RouteInfo = serde_json::from_str(&kv.value)?;
            res_route.route.push(value);
        }
        Ok(res_route)
    }
//    pub fn check_user_info(&self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>> {
//        let result = db.prefix_iterator(&PrefixTypeCode::UserInfo.prefix(), &CfNameTypeCode::SystemData.get())?;
//        for kv in result{
//            let user_info: UserInfo = serde_json::from_str(&kv.value)?;
//            if user_info.hook_id == self.hook_id {
//                return Ok(());
//            }
//        }
//        let err = format!("invalid hook_id: {}", &self.hook_id);
//        return Err(err.into());
//    }
}

///
/// 获取mysql路由信息
pub fn get_route_info(db: web::Data<DbInfo>, info: web::Json<GetRouteInfo>) -> HttpResponse {
    //let info = GetRouteInfo{hook_id: info.hook_id.clone(), clusters: info.clusters.clone()};
//    let v = info.get(&db);
//    match v {
//        Ok(rinfo) => {
//            return response_value(&rinfo);
//        }
//        Err(e) => {
//            return ResponseState::error(e.to_string());
//        }
//    }
    return ResponseState::ok()
}

impl PostCluster{
    fn get_route_info(&self, db: &web::Data<DbInfo>) -> Result<ResponseRouteInfo, Box<dyn Error>>{
        let mut res_route = ResponseRouteInfo{route: vec![]};
        let kv = db.prefix_get(&PrefixTypeCode::RouteInfo, &self.cluster_name)?;
        if kv.value.len() > 0 {
            let value: RouteInfo = serde_json::from_str(&kv.value)?;
            res_route.route.push(value);
        }
        Ok(res_route)
    }
}

///
///
/// web端拉取对应集群的路由信息
pub fn web_get_route_info(db: web::Data<DbInfo>, info: web::Json<PostCluster>) -> HttpResponse {
    let v = info.get_route_info(&db);
    match v {
        Ok(rinfo) => {
            return response_value(&rinfo);
        }
        Err(e) => {
            return ResponseState::error(e.to_string());
        }
    }
}


#[derive(Serialize, Deserialize)]
pub struct PostUserInfo {
    pub user_name: String,
    pub password: String
}

///
/// 新建用户
pub fn create_user(db: web::Data<DbInfo>, info: web::Form<PostUserInfo>) -> HttpResponse {
    let result =db.prefix_get(&PrefixTypeCode::UserInfo, &info.user_name);
    match result {
        Ok(tmp) => {
            if tmp.value.len() > 0 {
                let err = format!("user :{} already exists ", &info.user_name);
                return ResponseState::error(err);
            }
            let user_info = crate::storage::opdb::UserInfo::new(&info);
            return response_state(db.prefix_put(&PrefixTypeCode::UserInfo, &info.user_name, &user_info));
        }
        Err(e) => {
            return ResponseState::error(e.to_string());
        }
    }
}


///
/// 编辑用户信息
pub fn edit_user(db: web::Data<DbInfo>, info: web::Json<PostUserInfo>) -> HttpResponse {
    let result =db.prefix_get(&PrefixTypeCode::UserInfo, &info.user_name);
    match result {
        Ok(tmp) => {
            if tmp.value.len() > 0 {
                let user_info = crate::storage::opdb::UserInfo::new(&info);
                return response_state(db.prefix_put(&PrefixTypeCode::UserInfo, &info.user_name, &user_info));
            }
            let err = format!("user :{} not exists ", &info.user_name);
            return ResponseState::error(err);
        }
        Err(e) => {
            return ResponseState::error(e.to_string());
        }
    }
}


///
/// 登陆
pub fn login(db: web::Data<DbInfo>, info: web::Json<PostUserInfo>, session: Session) -> actix_web::Result<HttpResponse> {
    let result =db.prefix_get(&PrefixTypeCode::UserInfo, &info.user_name);
    match result {
        Ok(result) => {
            if result.value.len() == 0 {
                let err = format!("user: {} does not exist", &info.user_name);
                return Ok(ResponseState::error(err));
            }
            let userinfo: UserInfo = serde_json::from_str(&result.value)?;
            if userinfo.password != info.password {
                let err = format!("wrong password");
                return Ok(ResponseState::error(err));
            }
            session.set("username", &info.user_name)?;
            return Ok(ResponseState::ok());
        }
        Err(e) => {
            return Ok(ResponseState::error(e.to_string()));
        }
    }
}


#[derive(Serialize, Deserialize)]
pub struct PostUser{
    pub user_name: String
}
///
/// 退出登陆
pub fn loginout(info: web::Json<PostUser>, session: Session) -> actix_web::Result<HttpResponse> {
    session.remove(&info.user_name);
    Ok(ResponseState::ok())
}


#[derive(Serialize)]
pub struct GetUserInfo{
    pub user_name: String,
    pub hook_id: String,
    pub create_time: i64,
    pub update_time: i64,
}

impl GetUserInfo {
    fn new(user_info: &UserInfo) -> GetUserInfo {
        GetUserInfo{
            user_name : user_info.user_name.clone(),
            hook_id : user_info.hook_id.clone(),
            create_time: user_info.create_time.clone(),
            update_time: user_info.update_time.clone(),
        }
    }
}

///
/// 获取用户信息
pub fn get_user_info(db: web::Data<DbInfo>, session: Session) -> actix_web::Result<HttpResponse> {
    if let Some(username) = session.get::<String>("username")? {
        let result =db.prefix_get(&PrefixTypeCode::UserInfo, &username);
        match result {
            Ok(result) => {
                let value: UserInfo = serde_json::from_str(&result.value)?;
                let get_info = GetUserInfo::new(&value);
                return Ok(response_value(&get_info));
            }
            Err(e) => {
                return Ok(ResponseState::error(e.to_string()));
            }
        }
    }
    Ok(ResponseState::no_session())
}



#[derive(Serialize, Debug, Deserialize)]
pub struct GetSql{
    cluster_name: String
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ResponseSql {
    pub cluster_name: String,
    pub host: String,
    pub time: i64,
    pub number: u64,            //序号
    pub current: String,        //binlog原始sql
    pub rollback: String,       //binlog回滚语句
    pub carried: bool,          //是否已执行
    pub confirm: bool,          //是否已确认为正常
}
impl ResponseSql{
    fn new(info: &DifferenceSql) -> ResponseSql {
        ResponseSql{
            cluster_name: info.cluster.clone(),
            host: info.host.clone(),
            time: info.time.clone(),
            number: 0,
            current: "".to_string(),
            rollback: "".to_string(),
            carried: false,
            confirm: false
        }
    }

    fn append_sql_info(&mut self, info: &SqlRelation) {
        self.number = info.number.clone();
        self.current = info.current.clone();
        self.rollback = info.rollback.clone();
        self.carried = info.carried.clone();
        self.confirm = info.confirm.clone();
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct ResponseAllSql {
    total: usize,
    sql_info: Vec<ResponseSql>
}
impl ResponseAllSql {
    fn new() -> ResponseAllSql {
        ResponseAllSql{total: 0, sql_info: vec![] }
    }

    fn init_sql_info(&mut self, db: &web::Data<DbInfo>, info: &GetSql) -> Result<(), Box<dyn Error>> {
        let mut sql_vec = vec![];
        let mut total = 0 as usize;


        let result = db.get_rollback_sql(&info.cluster_name)?;
        for row in &result{
            let mut res_all = ResponseSql::new(&row.value);
            for sql_info in &row.value.sqls{
                res_all.append_sql_info(sql_info);
                sql_vec.push(res_all.clone());
                total += 1;
            }
        }

//        let prefix = PrefixTypeCode::RollBackSql.prefix();
//        let result = db.prefix_iterator(&prefix, &CfNameTypeCode::SystemData.get())?;
//        for row in result {
//            //info!("{:?}", &row);
//            if row.value.len() == 0 {continue;}
//            if info.cluster_name.len() > 0 {
//                let tmp = format!("{}:{}", &prefix, &info.cluster_name);
//                if !row.key.starts_with(&tmp){
//                    continue;
//                }
//            }
//
//            if row.key.starts_with(&prefix){
//                let value: DifferenceSql = serde_json::from_str(&row.value).unwrap();
//                let mut res_all = ResponseSql::new(&value);
//                for sql_info in &value.sqls{
//                    res_all.append_sql_info(sql_info);
//                    sql_vec.push(res_all.clone());
//                    total += 1;
//                }
//            }
//        }
        if sql_vec.len()>0{
            sql_vec.sort_by(|a, b| b.time.cmp(&a.time));
        }
        self.sql_info = sql_vec;
        self.total = total;
        Ok(())
    }
}

pub fn get_rollback_sql(db: web::Data<DbInfo>, info: web::Json<GetSql>) -> actix_web::Result<HttpResponse> {
    //info!("{:?}", &info);
    let mut re = ResponseAllSql::new();
    if let Err(e) = re.init_sql_info(&db, &info){
        return Ok(ResponseState::error(e.to_string()));
    };
    Ok(response_value(&re))
}



///
/// 需要追加的sql信息
#[derive(Serialize, Deserialize, Debug)]
pub struct PushSqlInfo{
    pub cluster_name: String,   //集群名
    pub host: String,
    pub time: i64,
    pub number: u64,
    pub sql: String             //binlog原始sql
}

#[derive(Serialize, Deserialize, Debug)]
pub struct PushSqlAll{
    pub sql_info: Vec<PushSqlInfo>
}

#[derive(Debug)]
pub struct ExtractSql{
    cluster_name: String,
    sqls: Vec<String>,
    markinfo: Vec<MarkSqlInfo>
}
impl ExtractSql{
    fn new() -> ExtractSql{
        ExtractSql{
            cluster_name: "".to_string(),
            sqls: vec![],
            markinfo: vec![]
        }
    }
}

#[derive(Debug)]
pub struct ExtractAll{
    pub info: Vec<ExtractSql>,
    pub success_cluster: Vec<String>
}
impl ExtractAll{
    fn new() -> ExtractAll{
        ExtractAll{
            info: vec![],
            success_cluster: vec![]
        }
    }
    fn extract(&mut self, info: &PushSqlInfo) {
        let cluster_name = info.cluster_name.clone();
        let sql = info.sql.clone();
        let mark_info = MarkSqlInfo{
            cluster_name:cluster_name.clone(),
            host: info.host.clone(),
            time: info.time.clone(),
            number: info.number.clone()
        };

        for extractsql in &mut self.info{
            if extractsql.cluster_name == cluster_name{
                extractsql.sqls.push(sql.clone());
                extractsql.markinfo.push(mark_info.clone());
                return;
            }
        }

        let mut extra = ExtractSql::new();
        extra.sqls.push(sql.clone());
        extra.markinfo.push(mark_info.clone());
        extra.cluster_name = cluster_name;
        self.info.push(extra);
    }

    fn execute(&mut self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>> {
        for extrac_info in &self.info{
            let master_host = self.get_master_info(&extrac_info.cluster_name, db)?;
            self.push_sql(&master_host, &extrac_info.sqls)?;
            self.success_cluster.push(extrac_info.cluster_name.clone());
            self.alter_sql_info_from_db(db, &extrac_info.markinfo)?;
        }
        Ok(())
    }


    fn get_master_info(&self, cluster_name: &String, db: &web::Data<DbInfo>) -> Result<String, Box<dyn Error>>{
        let route_result = db.prefix_get(&PrefixTypeCode::RouteInfo, cluster_name)?;
        let route_info: RouteInfo = serde_json::from_str(&route_result.value).unwrap();
        let result = db.iterator(&CfNameTypeCode::HaNodesInfo.get(), &String::from(""))?;
        for kv in result{
            if kv.value.len() == 0 {continue;}
            let value: HostInfoValue = serde_json::from_str(&kv.value).unwrap();
            if kv.key.starts_with(&route_info.write.host){
                if &value.cluster_name == cluster_name{
                    return Ok(kv.key);
                }
            }
        }
        let err = format!("could not get master information for cluster {}", cluster_name);
        return Err(err.into());
    }

    fn push_sql(&self, master_host: &String, sqls: &Vec<String>) -> Result<(), Box<dyn Error>> {
        let command_sql = CommandSql{ sqls: sqls.clone() };
        return MyProtocol::Command.push_sql(master_host, &command_sql);
    }

    fn alter_sql_info_from_db(&self, db: &web::Data<DbInfo>, mark_info: &Vec<MarkSqlInfo>) -> Result<(), Box<dyn Error>>{
        for mark in mark_info{
            mark.set_mark(db)?
        }
        Ok(())
    }
}

///
/// 追加回滚sql
pub fn push_sql(db: web::Data<DbInfo>, info: web::Json<PushSqlAll>) -> HttpResponse {
    let mut extra = ExtractAll::new();
    for info in &info.sql_info{
        extra.extract(info);
    }
    if let Err(e) = extra.execute(&db){
        let err = format!("success_cluster: {:?}, err: {:?}", &extra.success_cluster, e.to_string());
        return ResponseState::error(err);
    };

    ResponseState::ok()
}


#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MarkSqlInfo{
    pub cluster_name: String,   //集群名
    pub host: String,
    pub time: i64,
    pub number: u64
}
impl MarkSqlInfo{
    fn set_mark(&self, db: &web::Data<DbInfo>) -> Result<(), Box<dyn Error>>{
        let key = format!("{}:{}_{}", &self.cluster_name, &self.host, &self.time);
        let result = db.prefix_get(&PrefixTypeCode::RollBackSql, &key)?;
        let mut value: DifferenceSql = serde_json::from_str(&result.value).unwrap();
        value.alter(&db, &self.number)?;
        Ok(())
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct MarkSqlAll{
    pub sql_info: Vec<MarkSqlInfo>
}

///
/// 标记sql为已完成
pub fn mark_sql(db: web::Data<DbInfo>, info: web::Json<MarkSqlAll>) -> HttpResponse {
    for mark in &info.sql_info{
        if let Err(e) = mark.set_mark(&db){
            let err = format!("info: {:?} {}", &mark, e.to_string());
            return ResponseState::error(err);
        }
    }
    ResponseState::ok()
}


