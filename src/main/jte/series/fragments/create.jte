@import org.huebert.iotfsdb.schema.NumberType
@import org.huebert.iotfsdb.schema.PartitionPeriod

<script>
    function onNumberTypeChanged(self) {
      const minElement = document.getElementById('typeMinimum');
      const maxElement = document.getElementById('typeMaximum');
      if (self.value.startsWith('MAPPED') || self.value.startsWith('CURVED')) {
        minElement.style.display = 'block';
        maxElement.style.display = 'block';
      } else {
        minElement.style.display = 'none';
        maxElement.style.display = 'none';
        minElement.children[0].value = null;
        maxElement.children[0].value = null;
      }
    }
</script>
<h2>Create Series</h2>
<form>
    <fieldset>
        <label>
            ID
            <input name="id" placeholder="ID" autocomplete="series-id"/>
        </label>
        <label>
            Number Type
            <select name="type" aria-label="Select the number type..." required onchange="onNumberTypeChanged(this)">
                @for(NumberType type : NumberType.values())
                    <option selected="${type == NumberType.FLOAT4}">${type}</option>
                @endfor
            </select>
        </label>
        <label>
            Partition Period
            <select name="partition" aria-label="Select the partition period..." required>
                @for(PartitionPeriod period : PartitionPeriod.values())
                    <option selected="${period == PartitionPeriod.MONTH}">${period}</option>
                @endfor
            </select>
        </label>
        <label>
            Interval (ms)
            <input type="number" name="interval" placeholder="60000" autocomplete="series-interval" min="1" step="1"/>
        </label>
        <label id="typeMinimum" style="display:none">
            Minimum
            <input type="number" name="min" placeholder="-1.0" autocomplete="series-minimum"/>
        </label>
        <label id="typeMaximum" style="display:none">
            Maximum
            <input type="number" name="max" placeholder="1.0" autocomplete="series-maximum"/>
        </label>
    </fieldset>
    <input type="submit" value="Create" hx-post="/ui/series" hx-target="#main-body" hx-swap="innerHTML"
           hx-on::response-error="handleHtmxResponseError(event)"/>
</form>
