treat = require("treat")

function main(splash)
    splash.response_body_enabled = true
    splash:set_viewport_full()
    jsons = {}
    local url = splash.args.url

    splash:on_response(catchJson)
    assert(splash:go(url))
    assert(splash:wait(5))
    assert(splash:runjs("window.scrollTo(0,99999);"))
    assert(splash:wait(10))
    assert(splash:runjs("window.scrollTo(0,99999);"))
    assert(splash:wait(10))
    return jsons
end

function catchJson(response)
    if string.match(response.url, 'clientRequestID') then
        print(response.url)
        s,ct = treat.as_string(response.body)
        jsons[#jsons+1] = s
        local c = 0
        for i in string.gmatch (s, "commandLink") do
            c = c+1
        end;
        print(c)
    end
end
