@import gg.jte.Content

@param boolean secondary = false
@param boolean small = false
@param boolean visible = true
@param Content hxGet = null
@param Content hxPost = null
@param Content hxDelete = null
@param Content hxConfirm = null
@param String hxTarget = null
@param String hxSwap = "innerHTML"
@param Content content

<button class="btn ${secondary ? "btn-secondary" : "btn-primary"} ${small ? "btn-sm" : ""} ${visible ? "visible" : "invisible"} text-nowrap"
        hx-get="${hxGet}"
        hx-post="${hxPost}"
        hx-delete="${hxDelete}"
        hx-target="${hxTarget}"
        hx-swap="${hxSwap}"
        hx-confirm="${hxConfirm}"
        hx-on::response-error="handleHtmxResponseError(event)">
    ${content}
</button>
