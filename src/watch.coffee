fs = require 'fs'
winston = require 'winston'
memoize = require 'memoizee'
pth = require 'path'
Future = require('fibers/future')
Fiber = require 'fibers'
wait = Future.wait

config = fs.readJSONSync 'config.json'
location = config.cacheLocation
downloadLocation = pth.join(config.cacheLocation, "download")
uploadLocation = pth.join(config.cacheLocation, "upload")

maxCache = config.maxCacheSize  * 1024 * 1024

logger = new (winston.Logger)({
    transports: [
      new (winston.transports.Console)({ level: 'info' }),
      new (winston.transports.File)({ filename: '/tmp/BitcasaF4JS.log', level:'debug' })
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
readdir = Future.wrap(fs.readdir,1)
_statfs = (path, cb) ->
  fs.stat path, (err,attr)->
    cb(err,attr )
_memStatfs = Future.wrap(_statfs)
_memStatfs2 = (path) ->
  return _memStatfs(path).wait()
statfs = memoize(_memStatfs2, {maxAge:14400000} ) #remember for 4 hours
_unlink = (path, cb) ->
  fs.unlink path, ->
    cb(null, true)
unlink = Future.wrap(_unlink)
locked = false

watcher = fs.watch downloadLocation, (event, filename) ->
  logger.log("silly", "Watcher: event #{event} triggered by #{filename} - status: #{locked} - #{not locked}")
  if locked == false
    locked = true
    Fiber( ()->
      try
        #upload file sizes
        files = readdir(uploadLocation).wait()
        stats = (statfs(pth.join(uploadLocation,file)) for file in files)
        sizes = (stat.size for stat in stats)
        if sizes.length == 0
          totalUploadSize = 0
        else
          totalUploadSize = sizes.reduce (x,y) -> x + y

        #download file sizes
        files = readdir(downloadLocation).wait()
        stats = (statfs(pth.join(downloadLocation,file)) for file in files)
        sizes = (stat.size for stat in stats)
        if sizes.length == 0
          totalDownloadSize = 0
        else
          totalDownloadSize = sizes.reduce (x,y) -> x + y

        totalSize = totalUploadSize + totalDownloadSize

        if totalSize > maxCache
          #assume that files and stats are from the download directory
          all = zip(files,stats)
          all.sort(sortStats)


          for info in all
            if totalSize < maxCache
              break
            totalSize -= info[1].size
            unlink(pth.join(downloadLocation,info[0])).wait()
      catch error
        logger.log("debug", "Watcher: there was a problem: #{error}")
      locked = false
    ).run()
