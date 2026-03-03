export const TIMESTAMP_PATTERNS = {
    iso8601: /^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(?:[+-][0-9]{2}:?[0-9]{2})?$/,
    spaceSeparated: /^([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})$/,
    spaceSeparatedVariableHour: /^([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{1,2}):([0-9]{1,2}):([0-9]{1,2})$/,
    withoutSeconds: /^([0-9]{4})-([0-9]{2})-([0-9]{2})[T ]([0-9]{1,2}):([0-9]{2})$/,
    slashFormat: /^([0-9]{4})\/([0-9]{2})\/([0-9]{2}) ([0-9]{1,2}):([0-9]{1,2}):([0-9]{1,2})$/
};

export function formatBigQueryTimestamp(parts: { year: string; month: string; day: string; hour: string; minute: string; second: string }): string {
    const paddedHour = parts.hour.padStart(2, '0');
    const paddedMinute = parts.minute.padStart(2, '0');
    const paddedSecond = parts.second.padStart(2, '0');
    return `${parts.year}-${parts.month}-${parts.day} ${paddedHour}:${paddedMinute}:${paddedSecond}`;
}

export function convertTimestampToBigQueryFormat(val: string): string | null {
    if (!val || val === '') return null;
    
    const trimmed = val.trim().replace(/\s+/g, ' ');
    
    const isoMatch = trimmed.match(TIMESTAMP_PATTERNS.iso8601);
    if (isoMatch) {
        const [, year, month, day, hour, minute, second] = isoMatch;
        return formatBigQueryTimestamp({ year, month, day, hour, minute, second });
    }
    
    const spaceMatch = trimmed.match(TIMESTAMP_PATTERNS.spaceSeparated);
    if (spaceMatch) {
        const [, year, month, day, hour, minute, second] = spaceMatch;
        return formatBigQueryTimestamp({ year, month, day, hour, minute, second });
    }
    
    const spaceMatchVariable = trimmed.match(TIMESTAMP_PATTERNS.spaceSeparatedVariableHour);
    if (spaceMatchVariable) {
        const [, year, month, day, hour, minute, second] = spaceMatchVariable;
        return formatBigQueryTimestamp({ year, month, day, hour, minute, second });
    }
    
    const noSecondsMatch = trimmed.match(TIMESTAMP_PATTERNS.withoutSeconds);
    if (noSecondsMatch) {
        const [, year, month, day, hour, minute] = noSecondsMatch;
        return formatBigQueryTimestamp({ year, month, day, hour, minute, second: '00' });
    }

    const slashMatch = trimmed.match(TIMESTAMP_PATTERNS.slashFormat);
    if (slashMatch) {
        const [, year, month, day, hour, minute, second] = slashMatch;
        return formatBigQueryTimestamp({ year, month, day, hour, minute, second });
    }
    
    return val;
}

const TIMESTAMP_COLUMN_PATTERNS = [
    'time',
    'date',
    'timestamp',
    '_at',     // created_at, updated_at, deleted_at
    'created',
    'updated',
    'deleted',
    'modified'
];

export function isTimestampColumn(columnName: string): boolean {
    const lowerName = columnName.toLowerCase();
    return TIMESTAMP_COLUMN_PATTERNS.some(pattern => lowerName.includes(pattern));
}
