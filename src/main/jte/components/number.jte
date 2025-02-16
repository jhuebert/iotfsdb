@param String id
@param String label
@param Number min = null
@param Number step = null
@param Number value = null
@param boolean disabled = false
@param boolean small = false

<div class="col">
    <label class="text-nowrap" for="${id}">
        @if(small)
            <small>${label}</small>
        @else
            ${label}
        @endif
    </label>
    <input class="form-control ${small ? "form-control-sm" : ""}"
           id="${id}"
           type="number"
           name="${id}"
           min="${min}"
           disabled="${disabled}"
           step="${step}"
           value="${value}"/>
</div>
