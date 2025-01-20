@import java.time.ZonedDateTime
@import java.time.format.DateTimeFormatter

@param String id
@param String label = null
@param ZonedDateTime value
@param boolean disabled = false

!{DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");}

<div class="col">
    @if(label != null)
        <label for="${id}">${label}</label>
    @endif
    <input class="form-control"
           id="${id}"
           disabled="${disabled}"
           type="datetime-local"
           name="${id}"
           aria-label="${label}"
           value="${value != null ? value.toLocalDateTime().format(formatter) : null}">
</div>
