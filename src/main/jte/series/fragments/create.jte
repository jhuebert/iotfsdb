@import org.huebert.iotfsdb.api.schema.NumberType
@import org.huebert.iotfsdb.api.schema.PartitionPeriod

<script>
    function onNumberTypeChanged(self) {
        const isMappedOrCurved = self.value.startsWith('MAPPED') || self.value.startsWith('CURVED');
        document.getElementById('minimum').disabled = !isMappedOrCurved;
        document.getElementById('maximum').disabled = !isMappedOrCurved;
    }
</script>
<h4>Create Series</h4>
<form>
    <div class="row mb-3">
        @template.components.text(id = "id", label = "ID")
    </div>
    <div class="row mb-3">
        @template.components.select(
            id = "type",
            label = "Number Type",
            values = NumberType.values(),
            selected = NumberType.FLOAT4,
            onchange = "onNumberTypeChanged(this)"
        )
        @template.components.select(
            id = "partition",
            label = "Partition Period",
            values = PartitionPeriod.values(),
            selected = PartitionPeriod.MONTH
        )
    </div>
    <div class="row mb-3">
        @template.components.number(id = "interval", label = "Interval (ms)", min = 1, step = 60000, value = 60000)
        @template.components.number(id = "minimum", label = "Minimum", disabled = true)
        @template.components.number(id = "maximum", label = "Maximum", disabled = true)
    </div>
    <div class="row p-3">
        @template.components.button(content = @`Create`, hxPost = @`/ui/series`, hxTarget = "#main-body")
    </div>
</form>
