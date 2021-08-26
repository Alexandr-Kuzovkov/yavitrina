treat = require("treat")

function main(splash)
    splash.response_body_enabled = true
    splash:set_viewport_full()
    json = ''
    local url = splash.args.url

    splash:on_response(catchJson)
    assert(splash:go(url))
    assert(splash:wait(5))
    return json
end

function catchJson(response)
    if string.match(response.url, 'clientRequestID') then
        print(response.url)
        s,ct = treat.as_string(response.body)
        json = s
    end
end
