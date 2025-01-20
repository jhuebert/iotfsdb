@param String id
@param String label = null
@param Object[] values
@param Object selected
@param String onchange = null
@param boolean emptyOption = false

<div class="col">
    @if(label != null)
        <label for="${id}">${label}</label>
    @endif
    <select class="form-select" id="${id}" name="${id}" aria-label="Select the ${label}..." required
            onchange="${onchange}">
        @if(emptyOption)
            <option></option>
        @endif
        @for(Object value : values)
            <option selected="${value.equals(selected)}">${value.toString()}</option>
        @endfor
    </select>
</div>
