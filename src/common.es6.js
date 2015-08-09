"use strict";
/*
######################################
######### Setup File Config ##########
######################################
*/
const fs = require('fs-extra');
const pth = require('path');
const winston = require('winston');

var config = {};
if(fs.existsSync('config.json'))
  config = fs.readJSONSync('config.json');
if( !config.cacheLocation)
  config.cacheLocation =  "/tmp/cache";
if( !config.advancedChunks)
  config.advancedChunks = 5;
if(!config.chunkSize)
  config.chunkSize = 1024*1024*16;
var maxCache;
if(config.maxCacheSize){
   maxCache =  config.maxCacheSize * 1024 * 1024;
}else{
  logger.info( "max cache size was not set. you should exit and manually set it");
  logger.info( "defaulting to a 10 GB cache");
   maxCache = 10737418240;
}


// setup oauth client
const google = require('googleapis');
const GDrive = google.drive({ version: 'v2' });
const OAuth2Client = google.auth.OAuth2;
const oauth2Client = new OAuth2Client(config.clientId || "520595891712-6n4r5q6runjds8m5t39rbeb6bpa3bf6h.apps.googleusercontent.com"  , config.clientSecret || "cNy6nr-immKnVIzlUsvKgSW8", config.redirectUrl || "urn:ietf:wg:oauth:2.0:oob");
oauth2Client.setCredentials(config.accessToken);
google.options({ auth: oauth2Client });

// ensure directory exist for upload, download and data folders
const uploadLocation = pth.join(config.cacheLocation, 'upload');
fs.ensureDirSync(uploadLocation);
const downloadLocation = pth.join(config.cacheLocation, 'download');
fs.ensureDirSync(downloadLocation);
const dataLocation = pth.join(config.cacheLocation, 'data');
fs.ensureDirSync(dataLocation);

function printDate(){
  const d = new Date();
  return `${d.getFullYear()}-${d.getMonth()+1}-${d.getDate()}T${d.getHours()}:${d.getMinutes()}::${d.getSeconds()}`;
}
// setup winston logger

const transports = [new (winston.transports.File)({
  filename: '/tmp/GDriveF4JS.log',
  level:'debug' ,
  maxsize: 10485760, //10mb
  maxFiles: 3
})];
if(config.debug)
  transports.push(new (winston.transports.Console)({ level: 'debug', timestamp: printDate,colorize: true }));
else
  transports.push(new (winston.transports.Console)({ level: 'info', timestamp: printDate,colorize: true }));

const logger = new (winston.Logger)({
  transports: transports
});

var lockRefresh = false;
function refreshAccessToken(cb){
	//if not locked, refresh access token
	if(lockRefresh)
	{
		cb();
		return;
	}
	
	lockRefresh = true
	oauth2Client.refreshAccessToken( function refreshAccessTokenCallback(err,tokens){
		if(err)
		{
			// logger.debug "There was an error with refreshing access token"
			// logger.debug err
			refreshToken(cb);
			return;
		}

		config.accessToken = tokens;
	    fs.outputJson('config.json', config, function writeConfigCallback(err) {
	      if (err) {
	        logger.debug("failed to save config");            
	      } 
	      lockRefresh = false;
	      cb();
	    });
	  });
}


module.exports = {
	refreshAccessToken: refreshAccessToken,
	logger: logger,
	downloadLocation: downloadLocation,
	uploadLocation: uploadLocation,
	dataLocation: dataLocation,
	oauth2Client: oauth2Client,
	config: config,
	GDrive: GDrive,
	maxCache: maxCache

}