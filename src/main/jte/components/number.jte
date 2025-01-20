@param String id
@param String label
@param String style = null
@param Number min = null
@param Number max = null
@param Number step = null
@param Number value = null

<div class="col">
    <label for="${id}">${label}</label>
    <input class="form-control" style="${style}" id="${id}" type="number" name="${id}" min="${min}" max="${max}"
           step="${step}" value="${value}"/>
</div>