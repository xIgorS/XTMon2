(function () {
    function normalizeCellValue(text) {
        if (!text) {
            return '';
        }

        return text.replace(/\s+/g, ' ').trim();
    }

    function tryParseNumber(value) {
        if (!value) {
            return null;
        }

        var normalized = value
            .replace(/\s+/g, '')
            .replace(/,/g, '');

        var numericMatch = normalized.match(/^([-+]?\d*\.?\d+)(?:[a-zA-Z%]+)?$/);
        if (!numericMatch) {
            return null;
        }

        var parsed = Number(numericMatch[1]);
        return Number.isFinite(parsed) ? parsed : null;
    }

    function tryParseDate(value) {
        if (!value) {
            return null;
        }

        var dateTimeMatch = value.match(/^(\d{2})-(\d{2})-(\d{4})(?:\s+(\d{2}):(\d{2})(?::(\d{2}))?)?$/);
        if (!dateTimeMatch) {
            return null;
        }

        var day = Number(dateTimeMatch[1]);
        var month = Number(dateTimeMatch[2]) - 1;
        var year = Number(dateTimeMatch[3]);
        var hour = Number(dateTimeMatch[4] || 0);
        var minute = Number(dateTimeMatch[5] || 0);
        var second = Number(dateTimeMatch[6] || 0);
        var parsed = new Date(year, month, day, hour, minute, second);

        return Number.isNaN(parsed.getTime()) ? null : parsed.getTime();
    }

    function getComparableValue(cellText) {
        var normalized = normalizeCellValue(cellText);
        if (!normalized || normalized === '-') {
            return { type: 'empty', value: '' };
        }

        var numberValue = tryParseNumber(normalized);
        if (numberValue !== null) {
            return { type: 'number', value: numberValue };
        }

        var dateValue = tryParseDate(normalized);
        if (dateValue !== null) {
            return { type: 'date', value: dateValue };
        }

        return { type: 'text', value: normalized.toLocaleLowerCase() };
    }

    function compareValues(left, right) {
        if (left.type === 'empty' && right.type === 'empty') {
            return 0;
        }

        if (left.type === 'empty') {
            return 1;
        }

        if (right.type === 'empty') {
            return -1;
        }

        if (left.type === right.type) {
            if (left.value < right.value) {
                return -1;
            }

            if (left.value > right.value) {
                return 1;
            }

            return 0;
        }

        return String(left.value).localeCompare(String(right.value), undefined, { numeric: true, sensitivity: 'base' });
    }

    function sortTable(table, columnIndex, nextDirection) {
        var body = table.tBodies[0];
        if (!body) {
            return;
        }

        var indexedRows = Array.from(body.rows).map(function (row, originalIndex) {
            return { row: row, originalIndex: originalIndex };
        });

        indexedRows.sort(function (leftEntry, rightEntry) {
            var leftCell = leftEntry.row.cells[columnIndex];
            var rightCell = rightEntry.row.cells[columnIndex];
            var comparison = compareValues(
                getComparableValue(leftCell ? leftCell.innerText : ''),
                getComparableValue(rightCell ? rightCell.innerText : ''));

            if (comparison === 0) {
                return leftEntry.originalIndex - rightEntry.originalIndex;
            }

            return nextDirection === 'asc' ? comparison : -comparison;
        });

        indexedRows.forEach(function (entry) {
            body.appendChild(entry.row);
        });
    }

    function updateHeaderState(headers, activeHeader, direction) {
        headers.forEach(function (header) {
            var isActive = header === activeHeader;
            header.dataset.sortDirection = isActive ? direction : 'none';
            header.setAttribute('aria-sort', isActive ? (direction === 'asc' ? 'ascending' : 'descending') : 'none');
        });
    }

    function initializeTable(table) {
        if (!table || table.dataset.sortableInitialized === 'true') {
            return;
        }

        var headerRow = table.tHead && table.tHead.rows[0];
        var body = table.tBodies[0];
        if (!headerRow || !body) {
            return;
        }

        var headers = Array.from(headerRow.cells);
        if (headers.length === 0) {
            return;
        }

        headers.forEach(function (header, index) {
            header.classList.add('db-grid__head--sortable');
            header.tabIndex = 0;
            header.setAttribute('role', 'button');
            header.setAttribute('aria-sort', 'none');
            header.dataset.sortDirection = 'none';

            function triggerSort() {
                var nextDirection = header.dataset.sortDirection === 'asc' ? 'desc' : 'asc';
                updateHeaderState(headers, header, nextDirection);
                sortTable(table, index, nextDirection);
            }

            header.addEventListener('click', triggerSort);
            header.addEventListener('keydown', function (event) {
                if (event.key === 'Enter' || event.key === ' ') {
                    event.preventDefault();
                    triggerSort();
                }
            });
        });

        table.dataset.sortableInitialized = 'true';
    }

    function initializeSortableGrids(root) {
        var scope = root || document;
        if (!scope.querySelectorAll) {
            return;
        }

        scope.querySelectorAll('table.db-grid').forEach(initializeTable);
    }

    window.xtMonSortableGrid = {
        initialize: initializeSortableGrids
    };

    if (!window.__xtMonSortableGridObserverAttached) {
        window.__xtMonSortableGridObserverAttached = true;

        if (document.readyState === 'loading') {
            document.addEventListener('DOMContentLoaded', function () {
                initializeSortableGrids(document);
            });
        } else {
            initializeSortableGrids(document);
        }

        if (window.Blazor && typeof window.Blazor.addEventListener === 'function') {
            window.Blazor.addEventListener('enhancedload', function () {
                initializeSortableGrids(document);
            });
        }

        var observer = new MutationObserver(function (mutations) {
            mutations.forEach(function (mutation) {
                mutation.addedNodes.forEach(function (node) {
                    if (!(node instanceof HTMLElement)) {
                        return;
                    }

                    if (node.matches && node.matches('table.db-grid')) {
                        initializeTable(node);
                    }

                    initializeSortableGrids(node);
                });
            });
        });

        observer.observe(document.body, { childList: true, subtree: true });
    }
})();
