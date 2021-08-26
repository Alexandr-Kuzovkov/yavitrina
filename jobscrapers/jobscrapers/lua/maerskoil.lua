function main(splash)
    local url = splash.args.url
    assert(splash:go(url))
    assert(splash:wait(5.0))
    splash:set_viewport_full()
    jobs = {}
    local page = 1
    processing_page(splash, jobs)
    while next_page(splash, '#job_list_next') and page < 100 do
        processing_page(splash, jobs)
        page = page + 1
    end
    return jobs


end

function processing_page(splash, jobs)
    local dates = splash:select_all('.jobs_listing .left h4')
    local links = splash:select_all('.jobs_listing .job a')
    local areas = splash:select_all('.jobs_listing .functionalArea')
    local countries = splash:select_all('.jobs_listing .country h4')

    for i in ipairs(dates) do
        local job = {}
        job['date'] = dates[i]:text()
        job['title'] = links[i]:text()
        job['link'] = links[i].attributes['href']
        job['area'] = areas[i]:text()
        job['country'] = countries[i]:text()

        if (job['date'] ~= "${date}") then
            jobs[#jobs+1] = job
        end
    end
end

function next_page(splash, css_next_link)
    local next_link = splash:select(css_next_link)
    if next_link:exists() == true then
        next_link:mouse_click()
        splash:wait(5.0)
        return true
    else
        return false
    end
end