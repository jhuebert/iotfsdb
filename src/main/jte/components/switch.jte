@param String id
@param String label
@param boolean checked = false
@param boolean small = false

<div class="col">
    <div class="form-check form-switch">
        <input class="form-check-input" type="checkbox" role="switch" id="${id}" name="${id}" checked="${checked}">
        <label class="form-check-label" for="${id}">
            @if(small)
                <small>${label}</small>
            @else
                ${label}
            @endif
        </label>
    </div>
</div>