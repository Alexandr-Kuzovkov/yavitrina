function main(splash)
  	splash.resource_timeout = 20.0
    jobs = {}
    local url = splash.args.url
    assert(splash:go(url))
    assert(splash:wait(0.5))
    splash:set_viewport_full()
    processing_page(splash, jobs)
    local page = 1
  	while next_page(splash, '#requisitionListInterface\\.pagerDivID1786\\.Next') and page < 1 do
        processing_page(splash, jobs)
        page = page + 1
    end
    return jobs
end

function get_job(splash, element)
    assert(element:mouse_click())
    splash:wait(3.0)
    local back_link = splash:select('a#requisitionDescriptionInterface\\.backAction')
    local count = 0
   	while back_link:exists() == false and count < 10 do
        splash:wait(0.2)
        count = count+1
        back_link = splash:select('a#requisitionDescriptionInterface\\.backAction')
    end
    local job = {}
    local items = splash:select_all('.contentlinepanel')
    local j
  	for j in ipairs(items) do
        job[#job+1] = items[j]:text()
    end
    back_link:mouse_click()
    splash:wait(2.0)
    return job
end

function processing_page(splash, jobs)
    local pg = splash:select('#requisitionListInterface\\.pagerDivID1786\\.Label')
    print(pg:text())
    local links = splash:select_all('span.titlelink a')
  	local job
  	for k=1, #links do
        print(links[k]:text())
        job = get_job(splash, links[k])
        jobs[#jobs+1] = job
        links = splash:select_all('span.titlelink a')
    end
end

function next_page(splash, css_next_link)
    local next_link = splash:select(css_next_link)
    local attr = next_link.attributes['aria-disabled']
    if attr == "false" then
        next_link:mouse_click()
        splash:wait(2.0)
        return true
    else
        return false
    end
end
