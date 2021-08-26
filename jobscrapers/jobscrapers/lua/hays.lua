function main(splash, args)
  splash.resource_timeout = 60.0
    data = {}
    local url = splash.args.url
    assert(splash:go(url))
    assert(splash:wait(3.0))
    splash:set_viewport_size(1980, 1020)
    splash:set_user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36")
    width, height = splash:get_viewport_size()
    print(width)
    print(height)
  	splash:wait(2.0)
    scroll_to_end(splash)
    parse_jobs(splash, data)
    return data
end

function parse_jobs(splash, data)
    local links = splash:select_all('div.hays-job-title-decp > h2 > a')
    local titles = splash:select_all('div.hays-job-title-decp > h2 > a > span')
    local subtitles = splash:select_all('div.hays-job-title-decp > p')
    local j
  	for j in ipairs(links) do
        data[#data+1] = {link = links[j].attributes['href'], title = titles[j]:text(), subtitle = subtitles[j]:text()}
    end
end

function scroll_down(splash)
	local num_scrolls = 30
  local scroll_delay = 1.0
  local scroll_to = splash:jsfunc("window.scrollTo")
  local get_body_height = splash:jsfunc(
        "function() {return document.body.scrollHeight;}"
  )
   for _ = 1, num_scrolls do
        scroll_to(0, get_body_height())
        splash:wait(scroll_delay)
    end
end

function count_jobs(splash)
	  local items = splash:select_all('div.hays-job-title-decp')
    --print(#items)
    return #items
end

function scroll_to_end(splash)
	local prev_count_jobs = 0
  local curr_count_jobs = count_jobs(splash)
  while prev_count_jobs < curr_count_jobs do
    print("before")
    print(prev_count_jobs)
    print(curr_count_jobs)
    scroll_down(splash)
    prev_count_jobs = curr_count_jobs
    curr_count_jobs = count_jobs(splash)
    print("after")
    print(prev_count_jobs)
    print(curr_count_jobs)
  end
end


