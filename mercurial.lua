dofile("table_show.lua")
dofile("urlcode.lua")

local item_value = os.getenv("item_value")
local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false

if cgilua == nil then
  io.stdout:write("cgilua is not installed.\n")
  abortgrab = true
end

local listkeys = nil
local bundle2 = nil
local heads = nil
local done_main = false
local httpheadersize = 1024

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

run_external_wget = function(url, args, suffix)
  local check_s = "external" .. url .. args .. suffix
  if downloaded[check_s] then
    return nil
  end
  local command = "./wget-at" ..
    " -U 'mercurial/proto-1.0 (Mercurial 5.3.1)'" ..
    " --no-cookies" ..
    " --content-on-error" ..
    " --lua-script mercurial.lua" ..
    " -o '" .. item_dir .. "/wget" .. suffix .. ".log" .. "'" ..
    " --no-check-certificate" ..
    " --output-document '" .. item_dir .. "/wget" .. suffix .. ".tmp" .. "'" ..
    " --truncate-output" ..
    " -e robots=off" ..
    " --rotate-dns" ..
    " --recursive" ..
    " --level=inf" ..
    " --no-parent" ..
    " --page-requisites" ..
    " --timeout 30" ..
    " --tries inf" ..
    " --span-hosts" ..
    " --waitretry 30" ..
    " --warc-file '" .. item_dir .. "/" .. warc_file_base .. "-" .. suffix .. "'" ..
    " --warc-header 'operator: Archive Team'" ..
    " --warc-header 'warc-type: extra'" ..
    " --warc-dedup-url-agnostic"
  local header_index = 1
  local header_key = nil
  local temp_args = args
  local args_len = nil
  while string.len(temp_args) > 0 do
    header_key = 'x-hgarg-' .. tostring(header_index)
    args_len = 1024 - string.len(header_key) - 2
    command = command .. " --header '" .. header_key .. ": "
      .. string.sub(temp_args, 0, args_len) .. "'"
    temp_args = string.sub(temp_args, args_len+1, string.len(args))
    header_index = header_index + 1
  end
  command = command .. " '" .. url .. "'"
  local r = io.popen(command)
  for line in r:lines() do
    if not string.match(line, "%(external%)") then
      abortgrab = true
    end
    io.stdout:write(line .. "\n")
    io.stdout:flush()
  end
  r:close()
  downloaded[check_s] = true
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

  local function check_mercurial(cmd, args)
    local full_url = item_value .. "?cmd=" .. cmd .. "&" .. args
    if string.len(full_url) < 8100 then
      check(full_url)
    end
    run_external_wget(item_value .. "?cmd=" .. cmd, args, cmd)
  end

  if allowed(url, nil) and status_code == 200 then
    if string.match(url, "%?cmd=capabilities$") then
      local data = read_file(file)
      check(item_value .. "?cmd=heads")
      check(item_value .. "?cmd=hello")
      check(item_value .. "?cmd=stream_out")
      check_mercurial("listkeys", "namespace=namespaces")
      check_mercurial("batch", "cmds=" .. cgilua.urlcode.escape("heads ;known nodes="))
      check(item_value .. "?cmd=clonebundles")
      check(item_value .. "?cmd=branchmap")
      for capability in string.gmatch(data, "([^%s]+)") do
        if string.match(capability, "^bundle2=") then
          bundle2 = capability
        elseif string.match(capability, "^httpheader=") then
          httpheadersize = tonumber(string.match(capability, "^httpheader=([0-9]+)"))
        end
      end
    elseif string.match(url, "%?cmd=listkeys&namespace=namespaces") then
      listkeys = read_file(file)
    elseif string.match(url, "%?cmd=heads") then
      heads = string.match(read_file(file), "(.-)%s+$")
      --check_mercurial("changegroup", "roots=" .. string.gsub(heads, " ", "%%20"))
      check_mercurial("known", "nodes=" .. string.gsub(heads, " ", "%%20"))
      --check_mercurial("changegroupsubset", "bases=" .. string.gsub(heads, " ", "%%20"))
    elseif string.match(url, "%?cmd=clonebundles") then
      local data = read_file(file)
      for newurl in string.gmatch(data, "(https?://[^%s]+)") do
        check(newurl)
      end
    end
    if bundle2 ~= nil and listkeys ~= nil and heads ~= nil then
      namespaces = ""
      for namespace in string.gmatch(listkeys, "([^%s]+)") do
        if namespace ~= "obsolete" then
          if string.len(namespaces) ~= 0 then
            namespaces = namespaces .. ","
          end
          namespaces = namespaces .. namespace
        end
      end
      check_mercurial(
        "getbundle",
        "stream=1" ..
        "&cg=0" ..
        "&obsmarkers=1" ..
        "&bundlecaps=HG20," .. cgilua.urlcode.escape(bundle2) ..
        "&listkeys=" .. namespaces ..
        "&bookmarks=1"
      )
    end
  end

  return urls
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if string.match(url["url"], "%?cmd=capabilities$") then
    if status_code == 0 and err == 'AUTHFAILED' then
      return wget.actions.EXIT
    end
    done_main = true
  end

  if not done_main then
    io.stdout:write("(external) ")
  end
  
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. "  \n")
  io.stdout:flush()

  if abortgrab == true or downloaded[url["url"]] then
    io.stdout:write("ABORTING...\n")
    io.stdout:flush()
    return wget.actions.ABORT
  end

  downloaded[url["url"]] = true
  
  if status_code ~= 200 and status_code ~= 404 and status_code ~= 414 then
    io.stdout:write("Server returned "..http_stat.statcode.." ("..err.."). Sleeping.\n")
    io.stdout:flush()
    local maxtries = 2
    if tries > maxtries then
      io.stdout:write("\nI give up...\n")
      io.stdout:flush()
      io.stdout:write("ABORTING...\n")
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
    io.stdout:write("ABORTING...\n")
    io.stdout:flush()
    return wget.exits.IO_FAIL
  end
  return exit_status
end
