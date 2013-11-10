(function () {

    var ns = walker;

    $(document).ready(function () {
        var categoryElement = $('.category');
        var categoryId = categoryElement.data('category')
        var versionElements = categoryElement.find('.versions > div');
        versionElements.each(function () {
            var $this = $(this);
            var vid = $this.data('version');

            $.get('/api/category/' + vid + '/' + categoryId)
                .done(function (data) {
                    ns.buildGlyph($this, data);
                });
        });

        ns.idToggleButton();
    });

})();