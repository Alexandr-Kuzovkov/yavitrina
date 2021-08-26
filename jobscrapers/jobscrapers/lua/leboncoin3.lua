function main(splash, args)
    --splash:on_request(function(request)
    --        request:set_proxy{
    --            host = "tor",
    --            port = 9050,
    --            protocol = "socks5"
    --        }
    --end)
    --splash.proxy = 'socks5://tor:9050'
    splash.resource_timeout = 60.0
    data = {}
    local url = splash.args.url
    splash:set_viewport_size(1980, 1020)
    splash:set_user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.109 Safari/537.36")
    width, height = splash:get_viewport_size()
    print(width)
    print(height)
    assert(splash:go(url))
    assert(splash:wait(3.0))
    click_to_link(splash)
    splash:wait(3.0)
    parse_job(splash, data)
    for k, v in pairs(data) do
        print(k..'='..v)
    end
    --print(data)
    return data
end

function parse_job(splash, data)
    data['company'] = splash:select_all('div[data-qa-id="adview_contact_container"] div div')[3]:text()
    print(data['company'])
    if data['company'] == nil or data['company'] == '' then
        data['company'] = splash:select_all('span[data-qa-id="storebox_title"]')[1]:text()
    end
    local title = splash:select_all('div[data-qa-id="adview_title"] h1')
    if title then
        data['title'] = title[1]:text()
    end
    local salary = splash:select('div[data-qa-id="adview_price"] div span')
    if salary then
        data['salary'] = salary:text()
    end
    local desc = splash:select('div[data-qa-id="adview_description_container"] div span')
  	if desc then
        data['desc'] = desc.innerHTML
    end
    local contract_type = splash:select('div[data-qa-id="criteria_container"] div[data-qa-id="criteria_item_jobcontract"] div + div')
    if contract_type then
        data['contract_type'] =contract_type:text()
    end
    local category = splash:select('div[data-qa-id="criteria_container"] div[data-qa-id="criteria_item_jobfield"] div + div')
    if category then
        data['category'] = category:text()
    end
    local jobduty = splash:select('div[data-qa-id="criteria_container"] div[data-qa-id="criteria_item_jobduty"] div + div')
    if jobduty then
        data['jobduty'] = jobduty:text()
    end
    local experience = splash:select('div[data-qa-id="criteria_container"] div[data-qa-id="criteria_item_jobexp"] div + div')
    if experience then
        data['experience'] = experience:text()
    end
    local education = splash:select('div[data-qa-id="criteria_container"] div[data-qa-id="criteria_item_jobstudy"] div + div')
    if education then
        data['education'] = education:text()
    end
    local job_type = splash:select('div[data-qa-id="criteria_container"] div[data-qa-id="criteria_item_jobtime"] div + div')
    if job_type then
        data['job_type'] = job_type:text()
    end
    local location = splash:select('div[data-qa-id="adview_location_container"] div[data-qa-id="adview_location_informations"] span')
    if location then
        data['location'] = location:text()
    end

    data['raw1'] = ''
    data['raw2'] = ''
    data['raw3'] = ''
    data['raw4'] = ''
    local raw1 = splash:select('div[data-qa-id="adview_spotlight_description_container"]')
    if raw1 then
        data['raw1'] = raw1.innerHTML
    end
    local raw2 = splash:select('div[data-qa-id="adview_description_container"] div span[class="content-CxPmi"]')
    if raw2 then
        data['raw2'] = '<div>Description</div>'..raw2.innerHTML
    end
    local raw3 = splash:select('div[data-qa-id="criteria_container"]')
    if raw3 then
        data['raw3'] = '<div>Critères</div>'..raw3.innerHTML
    end
    local raw4 = splash:select('div[data-qa-id="adview_location_container"]')
    if raw4 then
        data['raw4'] = '<div>Localisation</div>'..raw4.innerHTML
    end

end


function click_to_link(splash)
    local link = splash:select('span[class="TextLink-15wnQ"]')
    if link then
        print('link was found')
        assert(splash:runjs("document.getElementsByClassName('TextLink-15wnQ')[0].click();"))
        splash:wait(2.0)
    end
end