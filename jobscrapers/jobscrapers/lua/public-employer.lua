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
    assert(splash:wait(3.0))
    click_to_plus(splash)
    get_jobs_links(splash, links)
   return links
end

function get_jobs_links(splash, links)
    local job_links = splash:select_all('ul[class="offers-list"] li a')
    for i=1, #job_links do
        print('add link ' .. job_links[i].attributes['href'])
        links[#links+1] = job_links[i].attributes['href']
    end
end

function count_jobs(splash)
    local job_links = splash:select_all('ul[class="offers-list"] li a')
    return #job_links
end

function mouse_click(splash)
    assert(splash:runjs("var button=document.getElementById('btn-load-more'); if (button != 'undefined') button.click();"))
end


function click_to_plus(splash)
  local limit = splash.args.limit
  local button = splash:select('button#btn-load-more')
  if button then
      print("button exists")
  end
  local prev_count_jobs = 0
  local curr_count_jobs = count_jobs(splash)
  local push_button = true
  while push_button do
        button = splash:select('button#btn-load-more')
        if button then
            mouse_click(splash)
            splash:wait(3.0)
            button = splash:select('button#btn-load-more')
            if button then
                print("button exists")
            end
        else
            print("button not found!")
            break
        end
        prev_count_jobs = curr_count_jobs
        curr_count_jobs = count_jobs(splash)
        print("number_jobs:")
        print('prev' .. prev_count_jobs)
        print('after' .. curr_count_jobs)
        if button then
            if curr_count_jobs > prev_count_jobs then
                push_button = true
            else
                mouse_click(splash)
                splash:wait(3.0)
                button = splash:select('button#btn-load-more')
                curr_count_jobs = count_jobs(splash)
                if curr_count_jobs > prev_count_jobs then
                    push_button = true
                else
                   push_button = false
                end
            end
        else
            push_button = false
        end
        if limit and curr_count_jobs >= limit then
            print('Limit is reached...')
            break
        end
  end
end
