/*
@author: xiao cai niao
@datetime: 2019/11/15
*/
pub mod route;

use actix_web::{http::header, HttpRequest, HttpResponse, Result};
use serde::{Deserialize, Serialize};
use actix_files::NamedFile;
use std::path::PathBuf;
use actix_session::Session;
use std::error::Error;

#[derive(Serialize, Deserialize)]
pub struct MyObj {
    pub name: String,
}

pub fn index(req: HttpRequest, session: Session) -> Result<HttpResponse> {
    let mut url = req.url_for("foo", &["index.html"])?; // <- generate url for "foo" resource
    if !session_check(session).unwrap() {
        url = req.url_for("login", &["login.html"])?;
    }
    Ok(HttpResponse::Found()
        .header(header::LOCATION, url.as_str())
        .finish())
}

pub fn index_static(req: HttpRequest, session: Session) -> Result<NamedFile> {
    if !session_check(session).unwrap(){
        return login();
    }
    let path: PathBuf = req.match_info().query("filename").parse().unwrap();
    Ok(NamedFile::open(path)?)
}

pub fn index_file(session: Session) -> Result<NamedFile>{
    if !session_check(session).unwrap(){
        return login();
    }
    Ok(NamedFile::open("index.html")?)
}

pub fn session_check(session: Session) -> Result<bool, Box<dyn Error>> {
    if let Some(_session) = session.get::<String>("username")? {
        return Ok(true);
    } else {
        return Ok(false);
    }
}

pub fn login() -> Result<NamedFile> {
    Ok(NamedFile::open("login.html")?)
}

