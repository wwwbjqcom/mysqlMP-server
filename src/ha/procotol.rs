/*
@author: xiao cai niao
@datetime: 2019/11/15
*/
use serde::Serialize;
use serde::Deserialize;
use std::net::TcpStream;
use std::error::Error;
use std::io::{Read, Write};
use crate::storage::opdb::HostInfoValue;
use crate::ha::nodes_manager::SlaveInfo;

#[derive(Debug, Serialize)]
pub enum  MyProtocol {
    MysqlCheck,
    GetMonitor,
    GetSlowLog,
    GetAuditLog,
    SetMaster,          //设置本机为新master
    ChangeMaster,
    PullBinlog,         //mysql服务宕机，拉取宕机节点差异binlog
    PushBinlog,         //推送需要追加的数据到新master
    RecoveryCluster,    //宕机重启(主动切换)自动恢复主从同步, 如有差异将回滚本机数据，并保存回滚数据
    GetRecoveryInfo,    //从新master获取宕机恢复同步需要的信息
    RecoveryValue,      //宕机恢复回滚的数据，回给服务端保存，有管理员人工决定是否追加
    ReplicationStatus,  //获取slave同步状态
    DownNodeCheck,      //宕机节点状态检查，用于server端检测到宕机时，分发到各client复检
    Ping,               //存活检查
    SetVariables,
    RecoveryVariables,
    Command,            //执行追加sql
    Ok,
    Error,
    UnKnow
}
impl MyProtocol {
    pub fn new(code: &u8) -> MyProtocol{
        if code == &0xfe {
            return MyProtocol::MysqlCheck;
        }else if code == &0xfd {
            return MyProtocol::GetMonitor;
        }else if code == &0xfc {
            return MyProtocol::GetSlowLog;
        }else if code == &0xfb {
            return MyProtocol::GetAuditLog;
        }else if code == &0xfa {
            return MyProtocol::SetMaster;
        }else if code == &0xf9 {
            return MyProtocol::ChangeMaster;
        }else if code == &0xf8 {
            return MyProtocol::PullBinlog;
        }else if code == &0xf7 {
            return MyProtocol::RecoveryCluster;
        }else if code == &0x00 {
            return MyProtocol::Ok;
        }else if code == &0x09 {
            return MyProtocol::Error;
        }else if code == &0xf6 {
            return MyProtocol::RecoveryValue;
        }else if code == &0xf5 {
            return MyProtocol::ReplicationStatus;
        }else if code == &0xf4 {
            return MyProtocol::DownNodeCheck;
        }else if code == &0xf3 {
            return MyProtocol::GetRecoveryInfo;
        }else if code == &0x04 {
            return MyProtocol::SetVariables;
        }else if code == &0x03 {
            return MyProtocol::RecoveryVariables;
        }else if code == &0xf2 {
            return MyProtocol::PushBinlog;
        }else if code == &0x01 {
            return MyProtocol::Ping;
        }else if code == &0x05 {
            return MyProtocol::Command;
        }
        else {
            return MyProtocol::UnKnow;
        }
    }

    pub fn get_code(&self) -> u8 {
        match self {
            MyProtocol::MysqlCheck => 0xfe,
            MyProtocol::GetMonitor => 0xfd,
            MyProtocol::GetSlowLog => 0xfc,
            MyProtocol::GetAuditLog => 0xfb,
            MyProtocol::SetMaster => 0xfa,
            MyProtocol::ChangeMaster => 0xf9,
            MyProtocol::PullBinlog => 0xf8,
            MyProtocol::PushBinlog => 0xf2,
            MyProtocol::RecoveryCluster => 0xf7,
            MyProtocol::Ok => 0x00,
            MyProtocol::Error => 0x09,
            MyProtocol::RecoveryValue => 0xf6,
            MyProtocol::ReplicationStatus => 0xf5,
            MyProtocol::DownNodeCheck => 0xf4,
            MyProtocol::GetRecoveryInfo => 0xf3,
            MyProtocol::SetVariables => 0x04,
            MyProtocol::RecoveryVariables => 0x03,
            MyProtocol::Ping => 0x01,
            MyProtocol::Command => 0x05,
            MyProtocol::UnKnow => 0xff
        }
    }

    ///
    /// 推送差异binlog到新master
    ///
    /// 返回sql事务数据
    ///
    pub fn push_binlog(&self, host: &String, buf: &BinlogValue) -> Result<RowsSql, Box<dyn Error>> {
        let packet = self.socket_io(host, buf)?;
        match packet.type_code {
            MyProtocol::RecoveryValue => {
                let value: RowsSql = serde_json::from_slice(&packet.value)?;
                return Ok(value);
            }
            MyProtocol::Error => {
                let err: ReponseErr = serde_json::from_slice(&packet.value)?;
                return Err(err.err.into());
            }
            _ => {
                let a = format!("return invalid type code:{:?}", &packet.type_code);
                return Err(a.into());
            }
        }
    }

    ///
    /// 拉取差异binlog数据
    pub fn pull_binlog(&self, host: &String, info: &SyncBinlogInfo) -> Result<BinlogValue, Box<dyn Error>> {
        let packet = self.socket_io(host, info)?;
        match packet.type_code {
            MyProtocol::PullBinlog => {
                let value: BinlogValue = serde_json::from_slice(&packet.value)?;
                return Ok(value);
            }
            MyProtocol::Error => {
                let err: ReponseErr = serde_json::from_slice(&packet.value)?;
                return Err(err.err.into());
            }
            _ => {
                let a = format!("return invalid type code:{:?}", &packet.type_code);
                return Err(a.into());
            }
        }
    }

    ///
    /// 推送需要执行的binlog到新master执行
    pub fn push_sql(&self, host: &String, info: &CommandSql) -> Result<(), Box<dyn Error>>{
        let packet = self.socket_io(host, info)?;
        return self.response_code_check(&packet);
    }

    ///
    /// 发送无数据的协议并返回数据，用于拉取数据
    ///
    pub fn get_packet(&self, host: &String) -> Result<RecPacket, Box<dyn Error>>{
        let packet_value = Null::new();
        let packet = self.socket_io(host, &packet_value)?;
        return Ok(packet);
    }

    ///
    ///
    pub fn change_master(&self, host: &String, buf: &ChangeMasterInfo) -> Result<(), Box<dyn Error>> {
        let packet = self.socket_io(host, buf)?;
        return self.response_code_check(&packet);
    }

    ///
    /// 宕机节点复检
    pub fn down_node_check(&self, host: &String, buf: &DownNodeCheck) -> Result<DownNodeCheckStatus, Box<dyn Error>> {
        let packet = self.socket_io(host, buf)?;
        self.response_is_self(&packet)?;
        let value: DownNodeCheckStatus = serde_json::from_slice(&packet.value)?;
        return Ok(value);
    }

    ///
    /// 宕机恢复
    pub fn recovery(&self, host: &String, buf: &RecoveryInfo) -> Result<RowsSql, Box<dyn Error>> {
        let packet = self.socket_io(host, buf)?;
        let mut v = RowsSql{ sqls: vec![], error: "".to_string(), etype: "".to_string() };
        match packet.type_code {
            MyProtocol::RecoveryValue => {
                info!("host: {} recovery success", &host);
                v= serde_json::from_slice(&packet.value)?;
            }
            MyProtocol::Error => {
                let e: ReponseErr = serde_json::from_slice(&packet.value)?;
                return Err(e.err.into());
            }
            MyProtocol::Ok => {
                info!("host: {} recovery success", &host);
            }
            _ => {
                let a = format!("return invalid type code:{:?}", &packet.type_code);
                return Err(a.into());
            }
        }
        return Ok(v);
    }

    ///
    /// 发送无数据内容的协议包
    /// 协议类型为自己
    ///
    /// 主要用于无数据内容的协议，并只返回ok、error包
    ///
    pub fn send_myself(&self, host: &String) -> Result<(), Box<dyn Error>> {
        let packet_value = Null::new();
        let packet = self.socket_io(host, &packet_value)?;
        return self.response_code_check(&packet);
    }

    ///
    /// 发送数据包，只接收返回的OK或者Erro包
    ///
    pub fn send_myself_value_packet<T: Serialize>(&self, host: &String, buf: &T) -> Result<(), Box<dyn Error>>{
        let packet = self.socket_io(host, buf)?;
        return self.response_code_check(&packet);
    }

    fn response_is_self(&self, packet: &RecPacket) -> Result<(), Box<dyn Error> >{
        if packet.type_code.get_code() == self.get_code() {
            return Ok(());
        }else {
            let a = format!("return invalid type code: {:?}",&packet.type_code);
            info!("{}", &a);
            return Err(a.into());
        }
    }

    fn response_code_check(&self, packet: &RecPacket) -> Result<(), Box<dyn Error>> {
        match packet.type_code {
            MyProtocol::Error => {
                let e: ReponseErr = serde_json::from_slice(&packet.value)?;
                return Err(e.err.into());
            }
            MyProtocol::Ok => {
                return Ok(());
            }
            _ => {
                let a = format!("return invalid type code: {:?}",&packet.type_code);
                return  Err(a.into());
            }
        }
    }

    pub fn socket_io<T: Serialize>(&self, host: &String, value: &T) -> Result<RecPacket, Box<dyn Error>> {
        let mut conn = crate::ha::conn(host)?;
        self.send_value_packet(&mut conn, value)?;
        let packet = self.rec_packet(&mut conn)?;
        return Ok(packet);
    }

    fn send_value_packet<T: Serialize>(&self, mut tcp: &TcpStream, value: &T) -> Result<(), Box<dyn Error>> {
        let value = serde_json::to_string(value)?;
        let mut buf = self.header(value.len() as u64);
        buf.extend(value.as_bytes());
        tcp.write(buf.as_ref())?;
        tcp.flush()?;
        Ok(())
    }

    fn header(&self,payload: u64) -> Vec<u8> {
        let mut buf: Vec<u8> = vec![];
        buf.push(self.get_code());
        let payload = crate::readvalue::write_u64(payload);
        buf.extend(payload);
        return buf;
    }

    ///
    /// 接收client返回的数据
    ///
    fn rec_packet(&self, conn: &mut TcpStream) -> Result<RecPacket, Box<dyn Error>> {
        let mut header: Vec<u8> = vec![0u8;9];
        conn.read_exact(&mut header)?;
        let payload = crate::readvalue::read_u64(&header[1..]);
        let mut payload_buf: Vec<u8> = vec![0u8; payload as usize];
        conn.read_exact(&mut payload_buf)?;
        let a = RecPacket{
            type_code: MyProtocol::new(&header[0]),
            payload,
            value: payload_buf
        };
        Ok(a)
    }
}

///
/// 返回的包
///
#[derive(Serialize)]
pub struct RecPacket{
    pub type_code: MyProtocol,
    pub payload: u64,
    pub value: Vec<u8>
}

///
/// 用于空包
///
#[derive(Serialize)]
pub struct Null {
    default: usize,
}
impl Null {
    pub fn new() -> Null {
        Null{ default: 0 }
    }
}

///
/// 错误信息
///
#[derive(Serialize, Deserialize)]
pub struct ReponseErr{
    pub err: String
}

///
/// client回复服务端状态检查的包
///
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct MysqlState {
    pub online: bool,
    pub role: String,
    pub master: String,
    pub sql_thread: bool,
    pub io_thread: bool,
    pub seconds_behind: usize,
    pub master_log_file: String,
    pub read_master_log_pos: usize,
    pub exec_master_log_pos: usize,
    pub read_only: bool,
    pub version: String,
    pub executed_gtid_set: String,
    pub innodb_flush_log_at_trx_commit: usize,
    pub sync_binlog: usize,
    pub server_id: usize,
    pub event_scheduler: String,
    pub sql_error: String
}
impl MysqlState{
    pub fn new() -> MysqlState{
        MysqlState{
            online: false,
            role: "".to_string(),
            master: "".to_string(),
            sql_thread: false,
            io_thread: false,
            seconds_behind: 0,
            master_log_file: "".to_string(),
            read_master_log_pos: 0,
            exec_master_log_pos: 0,
            read_only: false,
            version: "".to_string(),
            executed_gtid_set: "".to_string(),
            innodb_flush_log_at_trx_commit: 0,
            sync_binlog: 0,
            server_id: 0,
            event_scheduler: "".to_string(),
            sql_error: "".to_string()
        }
    }
}

///
/// 分发到client请求检查宕机节点状态
///
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DownNodeCheck {
    pub host: String,
    pub dbport: usize,
}
impl DownNodeCheck {
    pub fn new(host: String, dbport: usize) -> DownNodeCheck {
        DownNodeCheck{
            host,
            dbport
        }
    }
}

///
/// client回复检查宕机节点状态数据
///
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct DownNodeCheckStatus {
    pub host: String,
    pub client_status: bool,
    pub db_status: bool
}

impl DownNodeCheckStatus {
    pub fn new() -> DownNodeCheckStatus {
        DownNodeCheckStatus{
            host: "".to_string(),
            client_status: false,
            db_status: false
        }
    }
}

///
/// 用于master宕机对所有slave请求replication的情况，供选举以及追加binlog的作用
///
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ReplicationState {
    pub log_name: String,
    pub read_log_pos: usize,
    pub exec_log_pos: usize
}

///
/// master宕机后新master开始的binlog信息， 供恢复时建立同步使用
#[derive(Serialize, Deserialize, Debug)]
pub struct SetMasterStatus {
    pub log_name: String,
    pub log_pos: String,
    pub gtid: String
}

///
/// 服务端通过该协议请求client查询差异biogln
///
#[derive(Deserialize, Serialize, Debug)]
pub struct SyncBinlogInfo{
    pub binlog: String,
    pub position: usize
}
///
/// mysql实例宕机client所有差异的binlog原始数据
///
#[derive(Serialize, Deserialize)]
pub struct BinlogValue{
    pub value: Vec<u8>
}
///
/// 主从切换，指向到新master的基础信息
///
#[derive(Deserialize, Serialize, Debug)]
pub struct ChangeMasterInfo{
    pub master_host: String,
    pub master_port: usize
}
impl ChangeMasterInfo {
    pub fn new(host: String, port: usize) -> ChangeMasterInfo {
        let host_info = host.split(":");
        let host_vec = host_info.collect::<Vec<&str>>();
        ChangeMasterInfo{ master_host: host_vec[0].to_string(), master_port: port }
    }
}

///
/// 宕机恢复服务端发送恢复主从的基础信息
///
#[derive(Deserialize, Debug, Serialize)]
pub struct RecoveryInfo {
    pub binlog: String,
    pub position: usize,
    pub gtid: String,
    pub masterhost: String,
    pub masterport: usize,
    pub read_binlog: String,
    pub read_position: usize,
}

impl RecoveryInfo {
    pub fn new(node_info: &SlaveInfo) -> Result<RecoveryInfo, Box<dyn Error>> {
        let response_packet = MyProtocol::GetRecoveryInfo.get_packet(&node_info.host)?;
        let host_info = node_info.host.clone();
        let host_info = host_info.split(":");
        let host_vec = host_info.collect::<Vec<&str>>();

        match response_packet.type_code {
            MyProtocol::GetRecoveryInfo => {
                let info: GetRecoveryInfo = serde_json::from_slice(&response_packet.value)?;
                return Ok(RecoveryInfo{
                    binlog: info.binlog,
                    position: info.position,
                    gtid: info.gtid,
                    masterhost: host_vec[0].to_string(),
                    masterport: node_info.dbport.clone(),
                    read_binlog: node_info.slave_info.log_name.clone(),
                    read_position: node_info.slave_info.read_log_pos.clone()
                });
            }
            _ => {
                let a = format!("return invalid type code: {:?}",&response_packet.type_code);
                return  Err(a.into());
            }
        }
    }
}

///
/// 从新master获取的信息
///
#[derive(Deserialize, Debug, Serialize)]
pub struct GetRecoveryInfo {
    pub binlog: String,
    pub position: usize,
    pub gtid: String,
}

///
///用于宕机恢复旧master回滚，该结构体是从client发回的回滚等数据信息
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TractionValue{
    //pub event: Vec<Traction>,
    pub cur_sql: Vec<String>,
    pub rollback_sql: Vec<String>,
}
///
///用于宕机恢复旧master回滚，该结构体是从client发回的回滚等数据信息
///
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RowsSql {
    pub sqls: Vec<TractionValue>,
    pub error: String,
    pub etype: String,          //返回节点执行sql的类型， rollback或者append
}

///
/// 用于节点切换中保存节点信息
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct HostInfoValueGetAllState {
    pub host: String,   //127.0.0.1:3306
    pub dbport: usize,  //default 3306
    pub rtype: String,  //db、route
    pub online: bool,   //是否在线， true、false
    pub maintain: bool, //是否处于维护模式，true、false
    pub role: String,   //主从角色
    pub cluster_name: String,   //集群名称
}

impl HostInfoValueGetAllState {
    pub fn new(host_info: &HostInfoValue,role: String) -> HostInfoValueGetAllState {
        HostInfoValueGetAllState{
            host: host_info.host.clone(),
            dbport: host_info.dbport.clone(),
            rtype: host_info.rtype.clone(),
            online: host_info.online.clone(),
            maintain: host_info.maintain.clone(),
            cluster_name: host_info.cluster_name.clone(),
            role
        }
    }
}


///
/// 用于追加sql， 发送于客户端执行
#[derive(Deserialize, Serialize)]
pub struct CommandSql{
    pub sqls: Vec<String>
}

