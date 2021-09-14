function main(splash, args)
    splash.resource_timeout = 60.0
    links = {}
    local url = splash.args.url
    local limit = splash.args.limit
    splash:set_viewport_size(1980, 1020)
    splash.private_mode_enabled = false
    splash:set_user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36")
    splash.http2_enabled = true
    splash.indexeddb_enabled = true
    width, height = splash:get_viewport_size()
    print(width)
    print(height)
  	ok, reason = splash:go(url)
    splash:wait(10.0)
    scroll_to_end(splash)
    return splash:html()
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

function count_cards(splash)
	  local items = splash:select_all('div[class="products-list"] div.p-card')
    print(#items)
    return #items
end

function scroll_to_end(splash)
	local prev_count_jobs = 0
  local curr_count_jobs = count_cards(splash)
  while prev_count_jobs < curr_count_jobs do
    print("before")
    print(prev_count_jobs)
    print(curr_count_jobs)
    scroll_down(splash)
    prev_count_jobs = curr_count_jobs
    curr_count_jobs = count_cards(splash)
    print("after")
    print(prev_count_jobs)
    print(curr_count_jobs)
  end
end