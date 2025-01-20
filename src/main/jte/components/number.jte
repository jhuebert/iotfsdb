@param String id
@param String label
@param Number min = null
@param Number step = null
@param Number value = null
@param boolean disabled = false

<div class="col">
    <label for="${id}">${label}</label>
    <input class="form-control"
           id="${id}"
           type="number"
           name="${id}"
           min="${min}"
           disabled="${disabled}"
           step="${step}"
           value="${value}"/>
</div>
