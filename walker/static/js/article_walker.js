(function() {
    var ns = walker;

    $(document).ready(function(){
        var articleElement = $('.article');
        var articleId = articleElement.data('article')
        var versionElements = articleElement.find('.versions > div');
        versionElements.each(function() {
            var $this = $(this);
            var vid = $this.data('version');

            $.get('/api/article/' + vid + '/' + articleId)
                .done(function(data) {
                    ns.buildGlyph($this, data);
                });
        });

        ns.idToggleButton();
    });

})();