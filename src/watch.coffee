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
      new (winston.transports.Console)({ level: 'info' }),
      new (winston.transports.File)({ filename: '/tmp/GDriveFS.log', level:'debug' })
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

statfs = async.memoize(fs.statfs)

watcher = fs.watch downloadLocation, (event, filename) ->
  if locked == false
    locked = true
    #upload file sizes
    fs.readdir uploadLocation, (err, files) ->
      files = pth.join(uploadLocation,file) for file in files
      async.map files, statfs, (err, stats) ->
        if stats.length == 0
          totalUploadSize = 0
        else
          totalUploadSize = stats.reduce (x,y) -> x.size + y.size

        #download file sizes
        fs.readdir downloadLocation, (err, downloadFiles) ->
          async.map downloadFiles, statfs, (err, stats) ->

            if stats.length == 0
              totalDownloadSize = 0
            else
              totalDownloadSize = stats.reduce (x,y) -> x + y

            totalSize = totalUploadSize + totalDownloadSize

            if totalSize > maxCache
              #assume that files and stats are from the download directory
              all = zip(downloadFiles,stats)
              all.sort(sortStats)
              logger.debug "Watcher: event #{event} triggered by #{filename} - totalSize: #{totalSize})"
            else
              locked = false
              return


            for info in all
              if totalSize < 0.9*maxCache
                locked = false
                return
              totalSize -= info[1].size
              fs.unlinkSync(pth.join(downloadLocation,info[0]))
            locked = false
            return
          return
        return
      return
    return
  return