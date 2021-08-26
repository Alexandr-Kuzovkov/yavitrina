function main(splash)
  	splash.resource_timeout = 60.0
    jobs = {}
    local url = splash.args.url
    assert(splash:go(url))
    assert(splash:wait(0.5))
    splash:set_viewport_full()
    local job_link = splash:select('span.titlelink a')
    splash:wait(0.5)
    job_link:mouse_click()
  	splash:wait(2.0)
    parse_job(splash, jobs)
    local job_number = 1
    print('job_number:' .. tostring(job_number))
  	while next_job(splash) and job_number < 211 do
        parse_job(splash, jobs)
        job_number = job_number + 1
        print('job_number:' .. tostring(job_number))
    end
    return jobs
end

function parse_job(splash, jobs)
    local job = {}
    local item = splash:select('.contentlinepanel')
    print(item:text())
    local count = 0
    while item:exists() == false and count < 20 do
        splash:wait(1.0)
        count = count + 1
    end
    local items = splash:select_all('.contentlinepanel')
    local j
  	for j in ipairs(items) do
        job[#job+1] = items[j]:text()
    end
    jobs[#jobs+1] = job
end

function next_job(splash)
    local next_link = splash:select('#requisitionDescriptionInterface\\.pagerDivID820\\.Next')
  	local attr = next_link.attributes['aria-disabled']
    if attr == "false" then
        next_link:mouse_click()
        splash:wait(2.0)
        return true
    else
        return false
    end
end
