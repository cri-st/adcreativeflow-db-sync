export const TIMESTAMP_PATTERNS = {
    iso8601: /^([0-9]{4})-([0-9]{2})-([0-9]{2})T([0-9]{2}):([0-9]{2}):([0-9]{2})(?:[+-][0-9]{2}:?[0-9]{2})?$/,
    spaceSeparated: /^([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{2}):([0-9]{2}):([0-9]{2})$/,
    spaceSeparatedVariableHour: /^([0-9]{4})-([0-9]{2})-([0-9]{2}) ([0-9]{1,2}):([0-9]{2}):([0-9]{2})$/,
    withoutSeconds: /^([0-9]{4})-([0-9]{2})-([0-9]{2})[T ]([0-9]{1,2}):([0-9]{2})$/
};

export function formatBigQueryTimestamp(parts: { year: string; month: string; day: string; hour: string; minute: string; second: string }): string {
    const paddedHour = parts.hour.padStart(2, '0');
    return `${parts.year}-${parts.month}-${parts.day} ${paddedHour}:${parts.minute}:${parts.second}`;
}

export function convertTimestampToBigQueryFormat(val: string): string | null {
    if (!val || val === '') return null;
    
    const trimmed = val.trim();
    
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
    
    return val;
}
