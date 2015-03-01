fs = require 'fs-extra'
winston = require 'winston'
pth = require 'path'
async = require 'async'

config = fs.readJSONSync 'config.json'
location = config.cacheLocation
downloadLocation = pth.join(config.cacheLocation, "download")
uploadLocation = pth.join(config.cacheLocation, "upload")
if config.maxCacheSize
  maxCache =  config.maxCacheSize * 1024 * 1024 
else
  console.log "max cache size was not set. you should exit and manually set it"
  console.log "defaulting to a 10 GB cache"
  maxCache  = 10737418240  


logger = new (winston.Logger)({
    transports: [
      new (winston.transports.Console)({ level: 'debug' }),
      new (winston.transports.File)({ filename: '/tmp/GDriveF4JS.log', level:'debug' })
    ]
})

regexPattern = ///^[a-zA-Z0-9]*-([0-9]*)-([0-9]*)$///

getStats = (location, cb) ->
  fs.stat location, (err,stat) ->
    if err
      cb(err)
      return
    expectedSize = pth.basename(location).match(regexPattern)
    if(expectedSize == null)
      cb("size was null")
      return
    # console.log "Expected size for #{pth.basename(location)} is #{expectedSize[2]-expectedSize[1]}"
    stat.size = Math.max(parseInt(expectedSize[2])- parseInt(expectedSize[1]) + 1, 0)
    cb(err, stat)
    return
  return

memoizeStat = async.memoize( getStats )
zip = () ->
  lengthArray = (arr.length for arr in arguments)
  length = Math.min(lengthArray...)
  for i in [0...length]
    arr[i] for arr in arguments

sortStats = (x,y) ->
  diff = x[1].mtime.getTime() - y[1].mtime.getTime()
  switch
    when diff < 0 then return -1
    when diff == 0 then return 0
    else return 1

locked = false
watcher = fs.watch downloadLocation, (event, filename) ->
  logger.silly event
  if locked == false
    locked = true
    #upload file sizes
    logger.silly "checking cache size"
    logger.silly "getting list of upload files"
    fs.readdir uploadLocation, (err, files) ->
      logger.silly "files", files
      joined = ( pth.join(uploadLocation, file) for file in files)
      logger.silly "joined",joined
      logger.silly "getting sizes for uploaded files"
      async.map joined, fs.stat, (err, stats) ->
        totalUploadSize = 0
        unless stats.length == 0    
          for stat in stats
            try
              totalUploadSize += stat.size
            catch e
              logger.log stat
              continue

        logger.silly "total upload size is #{totalUploadSize}"
        #download file sizes
        logger.silly "getting download files"
        fs.readdir downloadLocation, (err, downloadFiles) ->

          downloadFiles = (pth.join(downloadLocation,file) for file in downloadFiles)

          async.map downloadFiles, memoizeStat, (err, stats) ->            
            totalDownloadSize = 0
            unless stats.length == 0
              for stat in stats
                totalDownloadSize += stat.size

            logger.silly "total download size is #{totalDownloadSize}"

            totalSize = totalUploadSize + totalDownloadSize
            logger.silly "total size is #{totalSize}"

            if totalSize > maxCache
              #assume that files and stats are from the download directory
              all = zip(downloadFiles,stats)
              all.sort(sortStats)
              logger.debug "Watcher: event #{event} triggered by #{filename} - totalSize: #{totalSize}) - maxCacheSize #{maxCache}"
            else
              logger.silly "totalSize was less than maxCache"
              locked = false
              return


            for info in all
              if totalSize < 0.9*maxCache
                locked = false
                return
              totalSize -= info[1].size
              path = pth.join(info[0])
              delete memoizeStat.memo[info[0]]
              logger.silly "deleting #{path}"
              fs.unlinkSync(path)
            locked = false
            return
          return
        return
      return
    return
  return