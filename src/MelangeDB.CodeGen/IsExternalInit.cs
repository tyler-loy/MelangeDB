// Polyfill so records and init-only setters compile against netstandard2.0, which Roslyn
// generators must target.
namespace System.Runtime.CompilerServices;

internal static class IsExternalInit;
