@import gg.jte.Content

@param boolean secondary = false
@param Content href = null
@param Content content

<a class="btn ${secondary ? "btn-secondary" : "btn-primary"}" href="${href}">
    ${content}
</a>
