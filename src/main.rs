/*
@author: xiao cai niao
@datetime: 2019/10/30
*/

use mymha;
use mymha::storage::rocks::DbInfo;


pub fn main() {
//    let mut db_info = DbInfo::new();
//    db_info.init_db();


    let db = DbInfo::new();
    db.init_admin_user().unwrap();
    mymha::start_web(db);

//    let cf_name = String::from("Nodes_state");
//    test(&cf_name, &db);

}

//fn test(cf_name: &String, db: &DbInfo) {
//    let result = db.iterator(cf_name, &String::from("")).unwrap();
//    for row in result{
//        println!("{:?}",row);
//    }
//}
//
//use serde::{Deserialize, Serialize};
//use serde_json::Result;
//
//#[derive(Serialize, Deserialize)]
//struct Address {
//    street: String,
//    city: String,
//}
//
//fn print_an_address() -> Result<()> {
//    // Some data structure.
//    let address = Address {
//        street: "10 Downing Street".to_owned(),
//        city: "London".to_owned(),
//    };
//
//    // Serialize it to a JSON string.
//    let j = serde_json::to_string(&address)?;
//
//    // Print, write to a file, or send to an HTTP server.
//    println!("{}", j);
//
//    Ok(())
//}