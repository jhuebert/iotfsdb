@import java.time.ZonedDateTime
@import java.time.format.DateTimeFormatter

@param String id
@param String label = null
@param ZonedDateTime value
@param boolean disabled = false
@param boolean small = false

<div class="col">
   @if(label != null)
       <label for="${id}" class="text-nowrap">${label}</label>
   @endif
   <input type="datetime-local"
          id="${id}"
          name="${id}"
          class="form-control ${small ? "form-control-sm" : ""}"
          aria-label="${label}"
          value="${value != null ? value.toLocalDateTime().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss")) : null}"
          ${disabled ? "disabled" : ""}>
</div>
