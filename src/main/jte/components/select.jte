@param String id
@param String label = null
@param Object[] values
@param Object selected
@param String onchange = null
@param boolean emptyOption = false
@param boolean small = false

<div class="col">
    @if(label != null)
        <label for="${id}">
            @if(small)
                <small>${label}</small>
            @else
                ${label}
            @endif
        </label>
    @endif
    <select class="form-select ${small ? "form-select-sm" : ""}" id="${id}" name="${id}" required
            onchange="${onchange}">
        @if(emptyOption)
            <option></option>
        @endif
        @for(Object value : values)
            <option selected="${value.equals(selected)}">${value.toString()}</option>
        @endfor
    </select>
</div>
