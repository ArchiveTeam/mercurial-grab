dofile("table_show.lua")
dofile("urlcode.lua")

local item_value = os.getenv('item_value')
local item_dir = os.getenv('item_dir')
local warc_file_base = os.getenv('warc_file_base')

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false

local listkeys = nil
local bundle2 = nil

for ignore in io.open("ignore-list", "r"):lines() do
  downloaded[ignore] = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

allowed = function(url, parenturl)
  return true
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  
  downloaded[url] = true

  local function check(urla)
    if string.len(urla) == 0 then
      return nil
    end
    local origurl = url
    local url = string.match(urla, "^([^#]+)")
    local url_ = string.gsub(string.match(url, "^(.-)%.?$"), "&amp;", "&")
    if (downloaded[url_] ~= true and addedtolist[url_] ~= true)
        and allowed(url_, origurl) then
      table.insert(urls, { url=url_ })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  if allowed(url, nil) and status_code == 200 then
    if string.match(url, "%?cmd=capabilities$") then
      local data = read_file(file)
      check(item_value .. "?cmd=heads")
      check(item_value .. "?cmd=hello")
      check(item_value .. "?cmd=stream_out")
      check(item_value .. "?cmd=listkeys&namespace=namespaces")
      check(item_value .. "?cmd=batch&cmds=heads ;known nodes=")
      check(item_value .. "?cmd=clonebundles")
      check(item_value .. "?cmd=branchmap")
      for capability in string.gmatch(data, "([^%s]+)") do
        if string.match(capability, "^bundle2=") then
          bundle2 = capability
        end
      end
    elseif string.match(url, "%?cmd=listkeys&namespace=namespaces") then
      listkeys = read_file(file)
    elseif string.match(url, "%?cmd=heads") then
      local data = string.match(read_file(file), "(.-)%s+$")
      check(item_value .. "?cmd=changegroup&roots=" .. data)
      --check(item_value .. "?cmd=changegroupsubset&bases=" .. data)
      check(item_value .. "?cmd=known&nodes=" .. data)
    elseif string.match(url, "%?cmd=clonebundles") then
      local data = read_file(file)
      check(string.match(data, "^([^%s]*)"))
    end
    if bundle2 ~= nil and listkeys ~= nil then
      namespaces = ""
      for namespace in string.gmatch(listkeys, "([^%s]+)") do
        if namespace ~= "obsolete" then
          if string.len(namespaces) ~= 0 then
            namespaces = namespaces .. ","
          end
          namespaces = namespaces .. namespace
        end
      end
      check(
        item_value ..
        "?cmd=getbundle" ..
        "&stream=1" ..
        "&cg=0" ..
        "&obsmarkers=1" ..
        "&bundlecaps=HG20," .. bundle2 ..
        "&listkeys=" .. namespaces ..
        "&bookmarks=1"
      )
    end
  end

  return urls
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  if abortgrab == true then
    io.stdout:write("ABORTING...\n")
    return wget.actions.ABORT
  end
  
  if status_code ~= 200 then
    io.stdout:write("Server returned "..http_stat.statcode.." ("..err.."). Sleeping.\n")
    io.stdout:flush()
    local maxtries = 10
    if tries > maxtries then
      io.stdout:write("\nI give up...\n")
      io.stdout:flush()
      tries = 0
      return wget.actions.ABORT
    else
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
      return wget.actions.CONTINUE
    end
  end

  tries = 0

  local sleep_time = 0

  if sleep_time > 0.001 then
    os.execute("sleep " .. sleep_time)
  end

  return wget.actions.NOTHING
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if abortgrab == true then
    return wget.exits.IO_FAIL
  end
  return exit_status
end
