@import gg.jte.Content

@param String value = null
@param Content hxPost
@param String hxTarget
@param boolean small = false

<div class="col">
    <div class="input-group">
        <input class="form-control ${small ? "form-control-sm" : ""}"
               name="search"
               type="search"
               placeholder="Search"
               value="${value}"/>
        @template.components.button(content = @`Search`, hxPost = hxPost, hxTarget = hxTarget, small = small)
    </div>
</div>