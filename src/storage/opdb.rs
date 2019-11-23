/*
@author: xiao cai niao
@datetime: 2019/11/6
*/
use actix_web::{web};
use crate::webroute::route::HostInfo;
use crate::storage::rocks::{DbInfo, KeyValue};


///
/// mysql node info， insert to rocksdb
///
///

pub fn insert_mysql_host_info(data: web::Data<DbInfo>, info: &web::Form<HostInfo>) -> Result<(), String> {
    let cf_name = String::from("Ha_nodes_info");
    let key = &info.host;

    let check_unique = data.get(key, &cf_name);
    match check_unique {
        Ok(v) => {
            if v.value.len() > 0 {
                return Err(format!("this key: ({}) already exists in the database",key).parse().unwrap());
            }
        }
        _ => {}
    }
    info!("{:?}",info);
    //info.create_time = crate::timestamp() as i64;
    let v = crate::ha::procotol::HostInfoValue{
        host: info.host.clone(),
        rtype: info.rtype.clone(),
        dbport: info.dbport.parse::<usize>().unwrap(),
        cluster_name: info.cluster_name.clone(),
        online: false,
        insert_time: crate::timestamp(),
        update_time: crate::timestamp(),
        maintain: false
    };

    let value = serde_json::to_string(&v).unwrap();
    let row = KeyValue{key: key.parse().unwrap(), value};
    let a = data.put(&row, &cf_name);
    match a {
        Ok(()) => Ok(()),
        Err(e) => {
            Err(e.to_string())
        }
    }

}