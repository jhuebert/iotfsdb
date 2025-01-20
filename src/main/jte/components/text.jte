@param String id
@param String label = null
@param String value = null
@param boolean disabled = false
@param String placeholder = null

<div class="col">
    @if(label != null)
        <label for="${id}">${label}</label>
    @endif
    <input class="form-control"
           id="${id}"
           name="${id}"
           value="${value}"
           disabled="${disabled}"
           autocomplete="${id}"
           placeholder="${placeholder}">
</div>
