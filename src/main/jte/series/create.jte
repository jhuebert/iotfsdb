@import org.huebert.iotfsdb.schema.NumberType
@import org.huebert.iotfsdb.schema.PartitionPeriod

<h2>Create Series</h2>
<form>
    <fieldset>
        <label>
            ID
            <input
                    name="series-id"
                    placeholder="ID"
                    autocomplete="series-id"
            />
        </label>
        <label>
            Number Type
            <select name="series-type" aria-label="Select the number type..." required>
                @for(NumberType type : NumberType.values())
                    @if(type == NumberType.FLOAT4)
                        <option selected>${type}</option>
                    @else
                        <option>${type}</option>
                    @endif
                @endfor
            </select>
        </label>
        <label>
            Partition Period
            <select name="series-partition" aria-label="Select the partition period..." required>
                @for(PartitionPeriod period : PartitionPeriod.values())
                    @if(period == PartitionPeriod.MONTH)
                        <option selected>${period}</option>
                    @else
                        <option>${period}</option>
                    @endif
                @endfor
            </select>
        </label>
        <label>
            Interval (ms)
            <input
                    type="number"
                    name="series-interval"
                    placeholder="60000"
                    autocomplete="series-interval"
                    min="1"
                    step="1"
            />
        </label>
        <label>
            Minimum
            <input
                    type="number"
                    name="series-minimum"
                    placeholder="-1.0"
                    autocomplete="series-minimum"
            />
        </label>
        <label>
            Maximum
            <input
                    type="number"
                    name="series-maximum"
                    placeholder="1.0"
                    autocomplete="series-maximum"
            />
        </label>
    </fieldset>
    <input
            type="submit"
            value="Save"
    />
</form>
