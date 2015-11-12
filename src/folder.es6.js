"use strict";

const fs = require( 'fs-extra');
const request = require( 'request');
const pth = require( 'path');

const common = require('./common.es6.js');
const config = common.config
const dataLocation = common.dataLocation;
const uploadLocation = common.uploadLocation;
const downloadLocation = common.downloadLocation;
const logger = common.logger;
const oauth2Client = common.oauth2Client;

// ######################################
// ######################################
// ######################################
class GFolder {
  constructor(id, parentid, name, ctime, mtime, permission, children, mode) {
    if (!children)
      children = [];
    if (!mode) {
      mode = 16895;//0o40777;
    }
    this.id = id;
    this.parentid = parentid;
    this.name = name;
    this.ctime = ctime; 
    this.mtime = mtime;
    this.permission = permission;
    this.children = children;
    this.mode = mode;
  }

  getAttrSync() {
    const attr = {
      mode: this.mode,
      size: 4096, //standard size of a directory
      nlink: this.children.length + 1,
      mtime: parseInt(this.mtime/1000),
      ctime: parseInt(this.ctime/1000),
      inode: this.inode
    };
    if(!attr.mode) attr.mode = 16895;
    return attr;
  }

  getAttr(cb) {
    const attr = this.getAttrSync();
    setImmediate(cb,0, attr);

  }
  
}

module.exports.GFolder = GFolder
