function main(splash, args)
  splash.resource_timeout = 60.0
    links = {}
    local url = splash.args.url
    splash:set_viewport_size(1980, 1020)
    splash:set_user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36")
    width, height = splash:get_viewport_size()
    print(width)
    print(height)
    assert(splash:go(url))
    assert(splash:wait(6.0))
    local header = splash:select('header.title h1.pivot'):text()
    local found = splash:select('header.title h2.figure'):text()
    local criterie = header .. "---" .. found
    scroll_to_end(splash)
    get_jobs_links(splash, links)
    return links, criterie
end


function get_jobs_links(splash, links)
    local header = splash:select('header.title h1.pivot'):text()
    local found = splash:select('header.title h2.figure'):text()
    local criterie = header .. "---" .. found
    local limit = splash.args.limit
    print('FILTER: ' .. criterie)
    local job_links = splash:select_all('div#SearchResults header h2 a')
    for i=1, #job_links do
        print('add link ' .. job_links[i].attributes['href'])
        links[#links+1] = job_links[i].attributes['href']
        if limit and #links >= limit then
            print('Limit is reached...')
            break
        end
    end
end

function count_jobs(splash)
	  local items = splash:select_all('div#SearchResults header h2 a')
    --print(#items)
    return #items
end

function click_to_link(splash)
    local link = splash:select('a#loadMoreJobs')
    if link then
        print('link was found')
        assert(splash:runjs("document.getElementById('loadMoreJobs').click();"))
        splash:wait(2.0)
    end
end

function scroll_to_end(splash)
    local prev_count_jobs = 0
    local curr_count_jobs = count_jobs(splash)
    local limit = splash.args.limit
    while prev_count_jobs < curr_count_jobs do
        print("before")
        print(prev_count_jobs)
        print(curr_count_jobs)
        click_to_link(splash)
        prev_count_jobs = curr_count_jobs
        curr_count_jobs = count_jobs(splash)
        print("after")
        print(prev_count_jobs)
        print(curr_count_jobs)
        if limit and curr_count_jobs >= limit then
            print('Limit is reached...')
            break
        end
  end
end