"use strict"
var google = require('googleapis');
var drive = google.drive({ version: 'v2' })
var readline = require('readline');
var fs = require('fs-extra');

var rest = require('restler');
var pth = require('path');

var folder = require("./folder.es6.js")
var GFolder = folder.GFolder
var f = require("./file.es6.js")
var GFile = f.GFile
var uploadTree = folder.uploadTree

var common = require('./common.es6.js');
var config = common.config
var dataLocation = common.dataLocation;
var uploadLocation = common.uploadLocation;
var downloadLocation = common.downloadLocation;
var logger = common.logger;


var __items_to_parse_from_google__ = []

/*
 *
 * Client Variables
 *
 */

var inodeTree = new Map();
var idToInode = new Map();

var now = (new Date).getTime();


var OAuth2Client = google.auth.OAuth2
var oauth2Client = new OAuth2Client(config.clientId || "520595891712-6n4r5q6runjds8m5t39rbeb6bpa3bf6h.apps.googleusercontent.com"  , config.clientSecret || "cNy6nr-immKnVIzlUsvKgSW8", config.redirectUrl || "urn:ietf:wg:oauth:2.0:oob")
var largestChangeId = 1;
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
            getPageFilesCallback = function getPageFilesCallback()
            {
                getPageFiles(pageToken, total, cb);
            }
            setTimeout(fn, 4000);
            return;
        }
        __items_to_parse_from_google__ = __items_to_parse_from_google__.concat(resp.items);
        var newTotal = total + resp.items.length;
        if(newTotal > 10000){
            newTotal -= 10000
            logger.info( `Taking a break from downloading files to try and parse files and folders. Current items to parse: ${__items_to_parse_from_google__.length}`)
            parseFilesFolders();
        }
        cb(null, newTotal, resp.nextPageToken);
    })
}



function getAllFiles(){
    function getAllFilesCallback(err, total, nextPageToken){
        logger.debug( `current length of items during downloading of all files is ${__items_to_parse_from_google__.length}`);
        if (nextPageToken){
            getPageFiles(nextPageToken, total, getAllFilesCallback);
        }
        else{
            // logger.log 'info', "Finished downloading folder structure from google"
            parseFilesFolders();
            // logger.debug __items_to_parse_from_google__
            saveFolderTree();
            getLargestChangeId();
            if(require.main != module)
                setTimeout(loadChanges, 90000);
        }

    }
    getPageFiles(null, 0, getAllFilesCallback);
}

function parseFilesFolders (){
    var items = __items_to_parse_from_google__;
    __items_to_parse_from_google__ = [];
    logger.debug( "Starting to parse items from google." );
    logger.debug( `There are ${items.length}  items to parse and the current inodeTree size is ${inodeTree.size}.` );
    var files = [];
    var folders = [];
    var root = inodeTree.get(1);
    var rootFound = false;
    if(root && root.id){
        rootFound = true;
    }

    var now = (new Date).getTime()

    var inodes = [];
    for ( let value of inodeTree.values() ){
        inodes.push(value.inode);
    }    
    var inodeCount = Math.max( Math.max.apply(null,inodes) + 1,2)
    logger.info("Parinsg data, looking for root foolder");
    // # google does not return the list of files and folders in a particular order.
    // # so find the root folder first,
    // # then parse the folders
    // # and then parse files

    fs.outputJsonSync(`${config.cacheLocation}/data/unparsed.json`, items);

    for( let i of items ){
        if ((! (i.parents) ) || i.parents.length == 0){
            continue
        }
        if(i.deleted || i.labels.trashed || i.labels.hidden){
            continue;
        }

        if(i.mimeType === "application/vnd.google-apps.folder"){
            if(!rootFound){
                if(i.parents[0].isRoot){
                    inodeTree.set(1, new GFolder(i.parents[0].id, null, 'root',now, now,1, true));
                    idToInode.set(i.parents[0].id, 1);
                    logger.info( "root node found");
                    rootFound = true;
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
            // # if (!f.parents ) or f.parents.length == 0
            // #   logger.log "debug", "folder.parents is undefined or empty"
            // #   logger.log "debug", f
            // #   continue
            var pid = f.parents[0].id //parent id
            var parentInode = idToInode.get(pid)
            if(parentInode){

                // if the parent exists, get it
                var parent = inodeTree.get(parentInode)

                // check to see if parent is a folder
                if (parent && parent instanceof GFolder){
                    if(!parent.hasOwnProperty( "children")){
                        parent.children = [];
                    }
                }else{
                    notFound.push(f);
                    continue;
                }

                // check to see if id has already been set
                if (idToInode.has(f.id)){
                    continue
                }

                idToInode.set( f.id, inodeCount);

                // push this current folder to the parent's children list
                if( parent.children.indexOf(inodeCount) < 0 ){
                    parent.children.push(inodeCount);
                    inodeTree.set(inodeCount, new GFolder(f.id, pid, f.title, (new Date(f.createdDate)).getTime(), (new Date(f.modifiedDate)).getTime(), inodeCount, f.editable , []));
                }
                inodeCount++
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
        var pid = f.parents[0].id;
        var parentInode = idToInode.get(pid);
        if (parentInode){
            var parent = inodeTree.get(parentInode)
            if( !parent.children ){
                continue
            }

            //add file to parent list
            parent.children.push(inodeCount);

            idToInode.set( f.id, inodeCount);
            inodeTree.set( inodeCount, new GFile(f.downloadUrl, f.id, pid, f.title, parseInt(f.fileSize), (new Date(f.createdDate)).getTime(), (new Date(f.modifiedDate)).getTime(), inodeCount, f.editable) );
            inodeCount++;
        }else{
            left.push(f)
        }
    }

    __items_to_parse_from_google__ = __items_to_parse_from_google__.concat(left)

    // logger.info "Finished parsing files"
    // logger.info "Everything should be ready to use"
    // saveFolderTree()
    logger.debug(`After attempting to parse, there is ${inodeTree.size} items in the inodeTree and ${__items_to_parse_from_google__.length} items that were not yet parseable`);
}

function parseFolderTreeInode(){
    const  jsonFile =  `${config.cacheLocation}/data/inodeTree.json`;
    const now = Date.now();
    fs.readJson( jsonFile, function readJsonFolderTreeCallback(err, data){
        try{
            for( let key of Object.keys(data) ){
                const o = data[key]
                const inode = o.inode;

                // add to idToPath
                idToPath.set(o.id,key);
                idToPath.set(o.parentid, pth.dirname(key));

                if( 'size' in o)
                    inodeTree.set( key, new GFile( o.downloadUrl, o.id, o.parentid, o.name, o.size, o.ctime, o.mtime, o.inode, o.permission ) );
                else
                    inodeTree.set( key, new GFolder(o.id, o.parentid, o.name, o.ctime, o.mtime, o.inode, o.permission,o.children) );

            }
            const changeFile = `${config.cacheLocation}/data/largestChangeId.json`;
            fs.exists( changeFile, function checkLargestChangeIdJsonExistsCallback(exists){
                if (exists){
                    fs.readJson(changeFile, function readLargestChangedIdCallback(err, data){                            
                        largestChangeId = data.largestChangeId;
                        if(require.main != module){
                            loadChanges();
                        }
                    });
                }
            });


        }catch(error){
            // if there was an error with reading the file, just download the whole structure again
            getAllFiles();
        }
    });
}

function parseFolderTree(){
    var jsonFile =  `${config.cacheLocation}/data/inodeTree.json`;
    var now = Date.now();
    fs.readJson( jsonFile, function readFolderTreeCallback(err, data){
        if(err){
            logger.debug(err)
            getAllFiles();
            return;
        }
        try{
            for( let key of Object.keys(data)){
                var o = data[key];
                if(key == "1"){
                    inodeTree.set( 1, new GFolder(o.id, o.parentid, o.name, o.ctime, o.mitime, o.inode, o.permission, o.children) )
                    idToInode.set( o.id, 1 );
                    continue;
                }

                // make sure parent directory exists
                if (!idToInode.has( o.parentid )){
                    logger.debug("parent directory did not exist");
                    logger.debug(o);
                    continue;
                }

                idToInode.set(o.id, o.inode);

                if('size' in o){
                    inodeTree.set(o.inode, new GFile( o.downloadUrl, o.id, o.parentid, o.name, o.size, o.ctime, o.mtime, o.inode, o.permission ));
                }else{
                    inodeTree.set( o.inode, new GFolder(o.id, o.parentid, o.name, o.ctime, o.mtime, o.inode, o.permission,o.children));
                }
            }

            var changeFile = `${config.cacheLocation}/data/largestChangeId.json`
            fs.exists( changeFile, function checkLargestChangeIdJsonExistsCallback(exists){
                if (exists){
                    fs.readJson(changeFile, function readLargestChangedIdCallback(err, data){
                        if (err)
                            largestChangeId = 0;
                        else
                            largestChangeId = data.largestChangeId
                        if( require.main != module){
                            loadChanges();
                        }
                    });
                }
            });
        }catch(error){
            // if there was an error with reading the file, just download the whole structure again
            logger.debug(error);
            getAllFiles();
        }
    });
}

function loadFolderTree(){
    // create (or read) the folderTree
    fs.exists(pth.join(dataLocation, 'inodeTree.json'), function checkFolderTreeExistsCallback(exists){
        logger.debug(`Folder tree exist status: ${exists}`);
        if (exists){
            logger.info("Loading folder structure");
            parseFolderTree();
        }else{
            logger.info( "Downloading full folder structure from google");
            getAllFiles();
        }
    });
}


var lockFolderTree = false;
function saveFolderTree(){
    if(!lockFolderTree){
        lockFolderTree = true
        logger.debug( "saving folder tree");
        var toSave = {};
        for( let key of inodeTree.keys()){
            var value = inodeTree.get(key);
            var saved = {
                downloadUrl: value.downloadUrl,
                id: value.id,
                parentid: value.parentid,
                name: value.name,
                ctime: value.ctime,
                mtime: value.mtime,
                inode: value.inode,
                permission: value.permission,
                mode: value.mode
            };

            if(value instanceof GFile){
                saved.size = value.size;
            }else{
                saved.children = value.children;
            }


            toSave[key] = saved;
        }

        fs.outputJson(pth.join(dataLocation,'inodeTree.json'), toSave,  function saveFolderTreeCallback(){});
        lockFolderTree = false;
    }
}


function getLargestChangeId(cb){
    var opts ={
        fields: "largestChangeId"
    };
    function getLargestChangeIdCallback(err, res){
        if( !err){
            largestChangeId = parseInt(res.largestChangeId) + 1;
            fs.outputJson(pth.join(dataLocation,"largestChangeId.json"), {largestChangeId:largestChangeId}, function(){});
        }
        cb();
    }
    drive.changes.list(opts, getLargestChangeIdCallback);
}

function loadPageChange(start, items, cb){

    const opts ={
        maxResults: 500,
        startChangeId: start
    };

    drive.changes.list( opts, function loadPageChangeCallback(err, res){
        if(!err){
            cb(err, res.largestChangeId, items.concat(res.items), res.nextPageToken);
        }else{
            logger.debug( "There was an error while loading changes" );
            logger.debug( err );
            cb(err, largestChangeId, items, start);
        }
    });
}


function loadChanges(cb){
    const id = largestChangeId;
    logger.debug(`Getting changes from Google Drive. The last change id was ${largestChangeId}.`)

    function loadChangesCallback(err, newId, items, pageToken){
        largestChangeId = newId
        if(pageToken){
            // drive.changes.list
            loadPageChange(pageToken, items, loadChangesCallback);
        }else{
            parseChanges(items);
        }
    }


    loadPageChange(id, [], loadChangesCallback);

}

function parseChanges(items){
    logger.debug( `There was ${items.length} to parse`);
    var notFound = [];
    logger.log(items);
    for(let i of  items){
      try{
        if( i.deleted || i.file.labels.trashed){ // check if it is deleted
            if( idToInode.has(i.fileId) ){ // check to see if the file was not already removed from folderTree
                logger.debug(`${i.file.title} was deleted`)
                var id = i.fileId;
                let inode = idToInode.get(id)
                var obj = inodeTree.get(inode)
                inodeTree.delete(inode);
                idToInode.delete(id);

                var parent = inodeTree.get(obj.parentid);
                if(!parent){
                    continue
                }
                var idx = parent.children.indexOf(inode);
                if(idx >= 0){
                    parent.children.splice(idx, 1)
                }
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
        var inode = idToInode.get(cfile.id)
        if(inode){
            const f = inodeTree.get(inode);
            logger.debug( `${f.name} was updated`);

            if(!f){
                idToPath.delete(path);
                notFound.push(i);
                continue;
            }
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
                f.parentid = cfile.parents[0].id;
            }
            continue;
        }

        if(cfile == undefined || cfile.parents == undefined || cfile.parents[0] == undefined){
            logger.debug ("changed file had empty parents");
            logger.debug (cfile);
            continue
        }

        var parentId = cfile.parents[0].id;
        var parentInode = idToInode.get(parentId);
        if(!parentInode){
            notFound.push(i);
            continue;
        }
        parent = inodeTree.get(parentInode);
        var inodes = []
        for( let value of inodeTree.values() ){
            inodes.push( value.inode );
        }
        
        inode = Math.max.apply(null, inodes) + 1
        idToInode.set(cfile.id, inode);
        parent.children.push(inode);
        if( cfile.mimeType == 'application/vnd.google-apps.folder'){
            logger.debug (`${cfile.title} is a new folder`);
            inodeTree.set( inode, new GFolder(cfile.id, parentId, cfile.title, (new Date(cfile.createdDate)).getTime(), (new Date(cfile.modifiedDate)).getTime(), inode, cfile.editable ));
        }else{
            logger.debug  (`${cfile.title} is a new file`)
            inodeTree.set (inode, new GFile(cfile.downloadUrl, cfile.id, parentId, cfile.title, parseInt(cfile.fileSize), (new Date(cfile.createdDate)).getTime(), (new Date(cfile.modifiedDate)).getTime(),inode, cfile.editable))
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
        saveFolderTree();
    }

    logger.debug("Finished parsing changes from google");
    setTimeout(loadChanges, config.refreshDelay + Math.random() * (config.refreshDelay) * 0.25);
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

module.exports.idToInode = idToInode
module.exports.inodeTree = inodeTree
module.exports.saveFolderTree = saveFolderTree
module.exports.drive = drive
module.exports.loadChanges = loadChanges
module.exports.parseFilesFolders = parseFilesFolders
