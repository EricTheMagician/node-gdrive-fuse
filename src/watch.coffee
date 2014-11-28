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
  console.log "max cache size was not. you should exit"


logger = new (winston.Logger)({
    transports: [
      new (winston.transports.Console)({ level: 'debug' }),
      new (winston.transports.File)({ filename: '/tmp/GDriveF4JS.log', level:'debug' })
    ]
})

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
      logger.silly files
      files = pth.join(uploadLocation,file) for file in files
      logger.silly "getting sizes for uploaded files"
      async.map files, fs.stat, (err, stats) ->
        totalUploadSize = 0
        unless stats.length == 0    
          for stat in stats
            try
              totalUploadSize += stat.size
            catch e
              continue

        logger.silly "total upload size is #{totalUploadSize}"
        #download file sizes
        logger.silly "getting download files"
        fs.readdir downloadLocation, (err, downloadFiles) ->

          downloadFiles = (pth.join(downloadLocation,file) for file in downloadFiles)

          async.map downloadFiles, fs.stat, (err, stats) ->            
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
              logger.silly "deleting #{path}"
              fs.unlinkSync(path)
            locked = false
            return
          return
        return
      return
    return
  return