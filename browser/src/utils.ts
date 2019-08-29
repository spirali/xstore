export function formatSize(value : number): string
{
    if (value < 512) {
        return value + "B";
    }
    if (value < 524288) {
        return (value / 1024).toFixed(2) + "KiB"
    }
    if (value < 536870912) {
        return (value / 1024 / 1024).toFixed(2) + "MiB"
    }
    return (value / 1024 / 1024 / 1024).toFixed(2) + "GiB"
}

export function formatTime(value : number): string
{
    if (value < 0.8) {
        return (value * 1000).toFixed(0) + "ms";
    }
    if (value < 60) {
        return value.toFixed(1) + "s";
    }
    if (value < 3600) {
        return (value / 60).toFixed(1) + "m";
    }
    return (value / 3600).toFixed(1) + "h";
}

export default formatSize;
