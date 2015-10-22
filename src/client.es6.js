"use strict"
const readline = require('readline');
const fs = require('fs-extra');

const pth = require('path');

const folder = require("./folder.es6.js")
const GFolder = folder.GFolder
const f = require("./file.es6.js")
const GFile = f.GFile
const uploadTree = folder.uploadTree

const common = require('./common.es6.js');
const config = common.config
const dataLocation = common.dataLocation;
const uploadLocation = common.uploadLocation;
const downloadLocation = common.downloadLocation;
const logger = common.logger;
const google = common.google;
const drive = common.GDrive;
const oauth2Client = common.oauth2Client;
const db = common.database;

const inodeTree = require('./inodetree.js');

var largestChangeId = 1;
var __items_to_parse_from_google__ = []
var numberOfInsertionsWhileInsertingIntoDatabse = 0;

/*
 *
 * Client functions
 *
 */

function getPageFiles(pageToken, total, cb)
{
    var opts = {
        fields: "etag,items(copyable,createdDate,downloadUrl,editable,fileExtension,fileSize,id,kind,labels(hidden,restricted,trashed),md5Checksum,mimeType,modifiedDate,parents(id,isRoot),shared,title,userPermission, version),nextPageToken",
        maxResults: 500,
        pageToken: pageToken
    }

    // logger.silly "current length of items during downloading of all files and folders is #{total} - left to parse: #{__items_to_parse_from_google__.length}"

    drive.files.list( opts, function getPageFilesCallback(err, resp){
        if(err)
        {
            logger.error ("There was an error while downloading files from google, retrying")
            logger.error (err)
            setTimeout(function getPageFilesErr(){getPageFiles(pageToken, total, cb);} , 4000);
            return;
        }
        let cmd = "INSERT OR REPLACE INTO folder_tree_creation (id, parentid, name, ctime, mtime, is_folder, is_root, size, is_editable, downloadUrl) VALUES "
        for( let item of resp.items){
            if(item.deleted || item.labels.trashed || item.labels.hidden){
                continue;
            }

            let is_folder = 0;
            let size = 0;
            let download_url = "";
            if( item.mimeType === "application/vnd.google-apps.folder" ){
                is_folder = 1;
            }else{                
                size = item.fileSize;
                download_url = item.downloadUrl;
            }

            let pid = "NULL";
            let is_root = 0;
            if( item.parents.length > 0){
                pid = item.parents[0].id
                if( item.parents[0].isRoot){
                    is_root = 1;
                }
            }

            let is_editable = 0;
            if( item.editable ){
                is_editable = 1;
            }

            //sometimes, the titles will contain null characters in the middle of string. remove them.
            let title = item.title.replace(/\0/g,'');


            cmd += `( "${item.id}", "${pid}", "${title}", ${(new Date(item.createdDate)).getTime()}, ${(new Date(item.modifiedDate)).getTime()}, ${is_folder}, ${is_root}, ${size}, ${is_editable}, "${download_url}"),`;
        }
        cmd = cmd.substring(0, cmd.length-1);
        numberOfInsertionsWhileInsertingIntoDatabse++;
        db.run(cmd, function insertInToDBFromParsingFiles(err){
            numberOfInsertionsWhileInsertingIntoDatabse--;
            if(err > 0){
                if(resp.items.length > 0){
                    logger.error(cmd);
                    logger.error("There was an error with inserting files in to the parsing database");
                    logger.error(err);                
                    return;
                }
            }


        });
        
        cb(null, total + resp.items.length, resp.nextPageToken);

    })
}


const number_to_parse_per_call = 5000;
function queryDatabaseAndParseFiles(offset){
    const cmd = `SELECT * from folder_tree_creation ORDER BY is_root DESC, is_folder DESC LIMIT ${number_to_parse_per_call} OFFSET ${offset}`;
    db.all( cmd, function callback(err, rows){
        if(err){
            logger.error(cmd);
            logger.error(err);
            setImmediate( ()=> { queryDatabaseAndParseFiles(offset) } );
            return;
        }
        if(rows.length == 0){
            inodeTree.saveFolderTree();
            return;
        }
        __items_to_parse_from_google__ = __items_to_parse_from_google__.concat(rows); 
        parseFilesFolders();
        queryDatabaseAndParseFiles(offset + number_to_parse_per_call);

    });

}


function getAllFiles(){
    function getAllFilesCallback(err, total, nextPageToken){
        logger.debug( `current length of items during downloading of all files is ${total}`);
        if (nextPageToken){
            getPageFiles(nextPageToken, total, getAllFilesCallback);
        }
        else{
            // logger.log 'info', "Finished downloading folder structure from google"
            queryDatabaseAndParseFiles(0);
            // logger.debug __items_to_parse_from_google__
            inodeTree.saveFolderTree();
            // findFoldersWithUnknownParents();
            // inodeTree.saveFolderTree();
            getLargestChangeId(function(){});
            if(require.main != module)
                setTimeout(loadChanges, 90000);
        }

    }
    getPageFiles(null, 0, getAllFilesCallback);
}

function parseFilesFolders (){

    if(numberOfInsertionsWhileInsertingIntoDatabse > 0){
        setTimeout(parseFilesFolders, 1000);
        return;
    }

    var items = __items_to_parse_from_google__;
    __items_to_parse_from_google__ = [];
    logger.debug( "Starting to parse items from google." );
    logger.debug( `There are ${items.length}  items to parse and the current inodeTree size is ${inodeTree.map.size}.` );
    var files = [];
    var folders = [];
    var root = inodeTree.getFromInode(1);
    var rootFound = false;

    if(root != null && root.id != null){
        rootFound = true;
        logger.info("Parinsg data");
    }else{
        logger.info("Parinsg data, looking for root foolder");
    }

    const now = (new Date).getTime()

    // # google does not return the list of files and folders in a particular order.
    // # so find the root folder first,
    // # then parse the folders
    // # and then parse files



    for( let i of items ){
        if (  i.parentid === "NULL"){
            if(rootFound){
                i.parentid = root.id;
            } else {
                notFound.push(i);
                continue;
            }
        }

        // if(i.deleted || i.labels.trashed || i.labels.hidden){
        //     continue;
        // }


        if(i.is_folder == 1){
            if(!rootFound){
                if(i.is_root){
                    let inode = inodeTree.insert( new GFolder(i.parentid, null, 'root',now, now, true));
                    logger.info( "root node found");
                    rootFound = true;
                    root = inodeTree.getFromInode(inode);
                }
            }

            folders.push(i);
        }else{
            files.push(i);
        }
    }

    var left = folders;
    while(left.length > 0){
        logger.info(`Folders left to parse: ${left.length}`)
        var notFound = [];

        for(let f of folders){
            // # if (!f.parfents ) or f.parents.length == 0
            // #   logger.log "debug", "folder.parents is undefined or empty"
            // #   logger.log "debug", f
            // #   continue
            if(inodeTree.getFromId(f.id) ){
                continue
            }

            const pid = f.parentid; //parent id
            const parent = inodeTree.getFromId(pid);
            if(parent){

                // check to see if parent is a folder
                if (!( parent instanceof GFolder)) {
                    // notFound.push(f);
                    logger.debug("possible error with parsing files");
                    logger.debug("parent:", parent);
                    logger.debug("child:", i);
                    continue;
                }



                // check to see if id has already been set
                // sometimes the file has come
                if (inodeTree.getFromId(f.id)){
                    continue;
                }

                // push this current folder to the parent's children list
                inodeTree.insert( new GFolder(f.id, pid, f.name, f.ctime, f.mtime, f.editable , []));
            }else{
                notFound.push(f)
            }

            //make sure that the folder list is gettting smaller over time.
        }
        if(left.length == notFound.length){
            logger.info(`There was ${left.length} folders that were not possible to process`);
            // logger.debug(notFound);
            break;
        }
        left = notFound;
    }

    logger.info("Parsing files");
    for(let f of files){
        if(inodeTree.getFromId(f.id) ){
            continue
        }

        const pid = f.parentid;
        const parent = inodeTree.getFromId(pid);
        if (parent){
            if( !parent.children ){
                continue;
            }

            //add file to parent list
            // debugger;
            const inode = inodeTree.insert( new GFile(f.downloadUrl, f.id, pid, f.name, f.size, f.ctime, f.mtime, f.editable) );

        }else{
            left.push(f);
        }
    }


    // logger.info "Finished parsing files"
    // logger.info "Everything should be ready to use"
    // saveFolderTree()
    __items_to_parse_from_google__ = left;
    logger.debug(`After attempting to parse, there is ${inodeTree.map.size} items in the inodeTree and ${__items_to_parse_from_google__.length} items that were not yet parseable`);
}


function loadFolderTreeCallback(err){
    if(err){
        getAllFiles();
        return;
    }
    const changeFile = pth.join(dataLocation,'largestChangeId.json');
    fs.exists( changeFile, function checkLargestChangeIdJsonExistsCallback(exists){
        if (exists){
            fs.readJson(changeFile, function readLargestChangedIdCallback(err, data){
                if (err)
                    largestChangeId = 1;
                else
                    largestChangeId = data.largestChangeId
                if( require.main != module){
                    loadChanges();
                }
            });
        }
    });

}

function loadFolderTree(){
    // create (or read) the folderTree
    fs.exists(pth.join(dataLocation, 'inodeTree.json'), function checkFolderTreeExistsCallback(exists){
        logger.debug(`Folder tree exist status: ${exists}`);
        if (exists){
            logger.info("Loading folder structure");
            inodeTree.loadFolderTree(loadFolderTreeCallback);
        }else{
            logger.info( "Downloading full folder structure from google");
            // queryDatabaseAndParseFiles(0);
            db.run("DROP TABLE IF EXISTS folder_tree_creation", function dropTableCallback(err){
                if(err){
                    logger.error("There was an error while droping table folder_tree_creation");
                    logger.error(err);
                }
                db.run(  "CREATE TABLE IF NOT EXISTS folder_tree_creation (id TEXT unique, parentid TEXT, name TEXT, ctime INT, mtime INT, is_folder INT, is_root INT, size INT,is_editable INT, downloadUrl TEXT)", function init_database_callback(err){
                  if (err){
                    logger.log (err)
                  }

                  getAllFiles();
                  

                });
            });
        }
    });
}




function getLargestChangeId(cb){
    var opts ={
        fields: "largestChangeId"
    };
    function getLargestChangeIdCallback(err, res){
        if( !err){
            largestChangeId = parseInt(res.largestChangeId);
            fs.outputJson(pth.join(dataLocation,"largestChangeId.json"), {largestChangeId:largestChangeId}, function(){});
        }
        cb();
    }
    drive.changes.list(opts, getLargestChangeIdCallback);
}

function loadPageChange(start, items, attempt, cb){

    const opts ={
        maxResults: 500,
        startChangeId: start
    };

    drive.changes.list( opts, function loadPageChangeCallback(err, res){
        if(!err){
            cb(err, parseInt(res.largestChangeId), items.concat(res.items), res.nextPageToken);
        }else{
            logger.debug( "There was an error while loading changes" );
            logger.debug( err );

            if(err){
                setTimeout( function(){loadPageChange(start, items, attempt + 1, cb);}, Math.random(config.refreshDelay)+config.refreshDelay * attempt);
            }
        }
    });
}


function loadChanges(cb){
    const id = parseInt(largestChangeId) + 1;
    logger.debug(`Getting changes from Google Drive. The last change id was ${largestChangeId}.`)

    function loadChangesCallback(err, newId, items, pageToken){
        largestChangeId = newId
        if(pageToken){
            loadPageChange(pageToken, items, 1, loadChangesCallback);
        }else{
            parseChanges(items);
        }
    }


    loadPageChange(id, [], 1, loadChangesCallback);

}

function parseChanges(items){
    logger.debug( `There was ${items.length} to parse`);
    var notFound = [];
    logger.log(items);
    for(let i of  items){
      try{
        if( i.deleted || i.file.labels.trashed){ // check if it is deleted
            let object = inodeTree.getFromId(i.fileId);
            if( object ){ // check to see if the file was not already removed from folderTree
                logger.debug(`${i.file.title} was deleted`);
                inodeTree.delete(object.inode);

            }else{
                try{
                    logger.debug( `processing a file that was marked as deleted, but not preset in the inodeTree: ${i.file.title} with id ${i.file.id}`);
                }catch (e){
                    logger.debug( `processfile a file that was marked as deleted but not present in the inodeTree` );
                    logger.debug( i );
                }
            }
            continue
        }

        const cfile = i.file // changed file
        if( !cfile){
            continue;
        }


        //if it is not deleted or trashed, check to see if it's new or not
        let f = inodeTree.getFromId(cfile.id)
        if(f){
            logger.debug( `${f.name} was updated`);

            f.ctime = (new Date(cfile.createdDate)).getTime()
            f.mtime = (new Date(cfile.modifiedDate)).getTime()
            if(f.name != cfile.title){
                logger.info( `${f.name} was renamed to ${cfile.title}`);
                f.name = cfile.title;
            }
            if(f instanceof GFile){
                f.downloadUrl = cfile.downloadUrl
            }

            //check that the file has parents
            if (!cfile.parents){ // cfile.parents.length == 0){
                continue
            }
            if (f.parentid != cfile.parents[0].id){
                logger.info (`${f.name} has moved`);
                inodeTree.setNewParents(f,cfile.parents[0].id);
            }
            continue;
        }

        if(cfile == undefined || cfile.parents == undefined || cfile.parents[0] == undefined){
            logger.debug ("changed file had empty parents");
            logger.debug (cfile);
            continue
        }

        var parentId = cfile.parents[0].id;
        var parentInode = inodeTree.getFromId(parentId);
        if(!parentInode){
            notFound.push(i);
            continue;
        }
        const parent = inodeTree.getFromInode(parentInode);
        common.currentLargestInode++;
        const node = common.currentLargestInode;
        if( cfile.mimeType == 'application/vnd.google-apps.folder'){
            logger.debug (`${cfile.title} is a new folder`);
            inodeTree.insert(new GFolder(cfile.id, parentId, cfile.title, (new Date(cfile.createdDate)).getTime(), (new Date(cfile.modifiedDate)).getTime(), cfile.editable ));
        }else{
            logger.debug  (`${cfile.title} is a new file`)
            inodeTree.insert(new GFile(cfile.downloadUrl, cfile.id, parentId, cfile.title, parseInt(cfile.fileSize), (new Date(cfile.createdDate)).getTime(), (new Date(cfile.modifiedDate)).getTime(), cfile.editable))
        }
      }catch(error){
              logger.debug("There was an error while parsing charges");
              logger.debug(error, i);
      }

    }
    if(notFound.length > 0 && notFound.length < items.length){
        parseChanges(notFound);
        return;
    }

    if(items.length > 0){
        fs.outputJson(`${config.cacheLocation}/data/largestChangeId.json`, {largestChangeId: largestChangeId}), function(){};
        inodeTree.saveFolderTree();
    }

    logger.debug("Finished parsing changes from google");
    setTimeout(loadChanges, config.refreshDelay + Math.random() * (config.refreshDelay) * 0.25);
}

function findFoldersWithUnknownParents(){
    var items = __items_to_parse_from_google__;
    var folders = [];
 
    for( let i of items ){
        if ((! (i.parents) ) || i.parents.length == 0){
            continue
        }
        if(i.deleted || i.labels.trashed || i.labels.hidden){
            continue;
        }

        if(i.mimeType === "application/vnd.google-apps.folder"){
            folders.push(i);
        }
    }
    const foldersWithUnkownParents = new Set();
    for(let f of folders){
        let found =false;
        for(let g of folders){
            for(let p of f.parents){
                if(p.id === g.id){
                    found = true;
                    break;
                }
            }
            if(found){
                break;
            }            

        }
        if(!found){
            // debugger;
            foldersWithUnkownParents.add(f.parents[0].id);
        }
    }    
    getParentsOfFoldersWithUnkownParents(foldersWithUnkownParents);    

}

function getParentsOfFoldersWithUnkownParents(foldersWithUnkownParents,cb){


    var finishedDownloadingFolders = 0;

    for(let folder of foldersWithUnkownParents){
        drive.files.get({fileId: folder}, function getNewFolderCallback(err,f){
            if(err){
                logger.error(err);
                drive.files.get({fileId: folder}, getNewFolderCallback);
                return;
            }
            finishedDownloadingFolders++;
            
            const rootid = inodeTree.getFromInode(1).parentid;
            var pid = rootid;
            if(f.parents.length > 0){
                pid = f.parents[0].id;
                if(!inodeTree.getFromId(pid)){
                    pid = rootid;
                }
            }

            const folder =  new GFolder(f.id, pid, f.title, (new Date(f.createdDate)).getTime(), (new Date(f.modifiedDate)).getTime(), f.editable , []);

            inodeTree.insert( folder);

            // console.log(file);
            if(finishedDownloadingFolders ==foldersWithUnkownParents.size ){
                parseFilesFolders();
            }

        }

        );

    }

}

/*
 ####################################
 ###### Setting up the Client #######
 ####################################
 */


var scopes = [
    'https://www.googleapis.com/auth/drive'
];

if(!config.accessToken){
    var url = oauth2Client.generateAuthUrl({
        access_type: 'offline', // 'online' (default) or 'offline' (gets refresh_token)
        scope: scopes, // If you only need one scope you can pass it as string
        approval_prompt: 'force' // #Force user to reapprove to get the refresh_token
    });
    console.log(url);

    // create interface to read access code
    var rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });

    rl.question( 'Enter the code here:', function askUserToInputAccessCodeCallback(code){
        // request access token
        oauth2Client.getToken(code, function getTokenCallback(err,tokens){
            oauth2Client.setCredentials(tokens);
            config.accessToken = tokens;
            common.config.accessToken = tokens;
            logger.info("Access Token Set")
            loadFolderTree();

            fs.outputJsonSync('config.json', config)
            return
        });
        rl.close();
    });


}else{
    oauth2Client.setCredentials(config.accessToken);
    logger.info( "Access Token Set" );
    loadFolderTree();
}

module.exports.loadChanges = loadChanges
