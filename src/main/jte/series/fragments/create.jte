@import org.huebert.iotfsdb.schema.NumberType
@import org.huebert.iotfsdb.schema.PartitionPeriod

<script>
    function onNumberTypeChanged(self) {
      const minElement = document.getElementById('minimum');
      const maxElement = document.getElementById('maximum');
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
    <div class="row mb-3">
        @template.components.text(id = "id", label = "ID")
    </div>
    <div class="row mb-3">
        @template.components.select(id = "type", label = "Number Type", values = NumberType.values(), selected = NumberType.FLOAT4)
        @template.components.select(id = "partition", label = "Partition Period", values = PartitionPeriod.values(), selected = PartitionPeriod.MONTH)
    </div>
    <div class="row mb-3">
        @template.components.number(id = "interval", label = "Interval (ms)", min = 1, step = 60000, value = 60000)
        @template.components.number(id = "minimum", label = "Minimum", style = "display:none")
        @template.components.number(id = "maximum", label = "Maximum", style = "display:none")
    </div>
    <div class="row p-3">
        @template.components.button(content = @`Create`, hxPost = @`/ui/series`, hxTarget = "#main-body")
    </div>
</form>
