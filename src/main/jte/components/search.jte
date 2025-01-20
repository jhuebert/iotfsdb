@import gg.jte.Content

@param String value = null
@param Content hxPost
@param String hxTarget

<div class="col">
    <div class="input-group">
        <input class="form-control" name="search" type="search" placeholder="Search" value="${value}"/>
        @template.components.button(content = @`Search`, hxPost = hxPost, hxTarget = hxTarget)
    </div>
</div>
